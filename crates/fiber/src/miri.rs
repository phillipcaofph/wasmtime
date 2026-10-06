//! A thread-based implementation of fibers, used to emulate fibers under Miri
//! and by the experimental thread-backed backend.
//!
//! Under Miri, this is an approximation: code running in a fiber does not share
//! TLS with the code managing the fiber.
//!
//! Each fiber keeps a worker thread to hold its execution stack. Resuming wakes
//! that worker while the caller waits; suspending parks the worker and returns
//! control to the caller.
//!
//! An issue was opened at rust-lang/miri#4392 for a possible extension to miri
//! to support stack-switching in a first-class manner.

use crate::{Result, RunResult, RuntimeFiberStack};
use std::boxed::Box;
use std::cell::Cell;
use std::io;
use std::mem;
use std::ops::Range;
#[cfg(wasmtime_thread_fibers)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[cfg(all(test, wasmtime_thread_fibers))]
std::thread_local! {
    static FAIL_NEXT_SPAWN: Cell<bool> = const { Cell::new(false) };
}

#[cfg(all(test, wasmtime_thread_fibers))]
pub(super) fn fail_next_spawn_for_test() {
    FAIL_NEXT_SPAWN.set(true);
}

#[cfg(all(test, wasmtime_thread_fibers))]
pub(super) fn stack_size_for_test(stack: &FiberStack) -> usize {
    stack.0
}

#[cfg(wasmtime_thread_fibers)]
static STARTED: AtomicU64 = AtomicU64::new(0);
#[cfg(wasmtime_thread_fibers)]
static LIVE: AtomicU64 = AtomicU64::new(0);

#[cfg(wasmtime_thread_fibers)]
#[unsafe(no_mangle)]
pub extern "C" fn wasmtime_thread_fiber_started() -> u64 {
    STARTED.load(Ordering::SeqCst)
}

#[cfg(wasmtime_thread_fibers)]
#[unsafe(no_mangle)]
pub extern "C" fn wasmtime_thread_fiber_live() -> u64 {
    LIVE.load(Ordering::SeqCst)
}

pub use wasmtime_environ::error::Error;

pub struct FiberStack(usize);

impl FiberStack {
    pub fn new(size: usize, _zeroed: bool) -> Result<Self> {
        #[cfg(wasmtime_thread_fibers)]
        if size > isize::MAX as usize {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        Ok(FiberStack(size))
    }

    pub unsafe fn from_raw_parts(_base: *mut u8, _guard_size: usize, _len: usize) -> Result<Self> {
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }

    pub fn is_from_raw_parts(&self) -> bool {
        false
    }

    pub fn from_custom(_custom: Box<dyn RuntimeFiberStack>) -> Result<Self> {
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }

    pub fn top(&self) -> Option<*mut u8> {
        None
    }

    pub fn range(&self) -> Option<Range<usize>> {
        None
    }

    pub fn guard_range(&self) -> Option<Range<*mut u8>> {
        None
    }
}

pub struct Fiber {
    state: *const u8,
    thread: Option<JoinHandle<()>>,
}

pub struct Suspend {
    state: *const u8,
}

/// Shared state, inside an `Arc`, between `Fiber` and `Suspend`.
struct SharedFiberState<A, B, C> {
    cond: Condvar,
    state: Mutex<State<A, B, C>>,
}

enum State<A, B, C> {
    /// No current state, or otherwise something is waiting for something else
    /// to happen.
    None,

    /// The fiber is being resumed with this result.
    ResumeWith(RunResult<A, B, C>),

    /// The fiber is being suspended with this result
    SuspendWith(RunResult<A, B, C>),

    /// The fiber needs to exit (part of drop).
    Exiting,
}

#[cfg(not(wasmtime_thread_fibers))]
unsafe impl<A, B, C> Send for State<A, B, C> {}
#[cfg(not(wasmtime_thread_fibers))]
unsafe impl<A, B, C> Sync for State<A, B, C> {}

struct IgnoreSendSync<T>(T);

unsafe impl<T> Send for IgnoreSendSync<T> {}
#[cfg(not(wasmtime_thread_fibers))]
unsafe impl<T> Sync for IgnoreSendSync<T> {}

#[cfg(wasmtime_thread_fibers)]
pub trait ThreadMessage: Send {}
#[cfg(wasmtime_thread_fibers)]
impl<T: Send> ThreadMessage for T {}
#[cfg(not(wasmtime_thread_fibers))]
pub trait ThreadMessage {}
#[cfg(not(wasmtime_thread_fibers))]
impl<T> ThreadMessage for T {}

fn run<F, A, B, C>(state: Arc<SharedFiberState<A, B, C>>, func: IgnoreSendSync<F>)
where
    F: FnOnce(A, &mut super::Suspend<A, B, C>) -> C,
    A: ThreadMessage,
    B: ThreadMessage,
    C: ThreadMessage,
{
    #[cfg(wasmtime_thread_fibers)]
    {
        STARTED.fetch_add(1, Ordering::SeqCst);
        LIVE.fetch_add(1, Ordering::SeqCst);
    }
    #[cfg(wasmtime_thread_fibers)]
    struct LiveThread;
    #[cfg(wasmtime_thread_fibers)]
    impl Drop for LiveThread {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[cfg(wasmtime_thread_fibers)]
    let _live = LiveThread;
    // Wait for the initial message of what to initially invoke `func` with.
    let init = {
        let mut lock = state.state.lock().unwrap();
        lock = state
            .cond
            .wait_while(lock, |msg| {
                !matches!(msg, State::ResumeWith(_) | State::Exiting)
            })
            .unwrap();
        match mem::replace(&mut *lock, State::None) {
            State::ResumeWith(RunResult::Resuming(init)) => init,
            State::Exiting => return,
            _ => unreachable!(),
        }
    };

    // Execute this fiber through `Suspend::execute` and once that's done
    // deallocate the `state` that we have.
    let state = Arc::into_raw(state);
    let mut suspend = super::Suspend::<A, B, C>::execute(
        Suspend {
            state: state.cast(),
        },
        init,
        func.0,
    );
    match suspend.block_until_notified::<A, B, C>() {
        State::Exiting => {}
        _ => unreachable!(),
    }
    unsafe {
        drop(Arc::from_raw(state));
    }
}

impl Fiber {
    pub fn new<F, A, B, C>(stack: &FiberStack, func: F) -> Result<Self>
    where
        F: FnOnce(A, &mut super::Suspend<A, B, C>) -> C,
        A: ThreadMessage,
        B: ThreadMessage,
        C: ThreadMessage,
    {
        // Allocate shared state between the fiber and the suspension argument.
        let state = Arc::new(SharedFiberState::<A, B, C> {
            cond: Condvar::new(),
            state: Mutex::new(State::None),
        });

        // The unchecked spawn is covered by the fiber wrapper's transfer
        // contract: thread-backed callers must ensure the closure and its
        // captures are safe to use on the worker until it is joined. Miri uses
        // this thread-based implementation to approximate stack switching.
        let worker = {
            let state = state.clone();
            let func = IgnoreSendSync(func);
            move || run(state, func)
        };
        #[cfg(all(test, wasmtime_thread_fibers))]
        if FAIL_NEXT_SPAWN.replace(false) {
            drop(worker);
            return Err(io::Error::other("injected worker spawn failure").into());
        }
        let thread = unsafe {
            thread::Builder::new()
                .stack_size(stack.0)
                .spawn_unchecked(worker)?
        };

        // Cast the fiber back into a raw pointer to lose the type parameters
        // which our storage container does not have access to. Additionally
        // save off the thread so the dtor here can join the thread.
        Ok(Fiber {
            state: Arc::into_raw(state).cast(),
            thread: Some(thread),
        })
    }

    pub(crate) fn resume<A, B, C>(&self, _stack: &FiberStack, result: &Cell<RunResult<A, B, C>>) {
        let my_state = unsafe { self.state() };
        let mut lock = my_state.state.lock().unwrap();

        // Swap `result` into our `lock`, then wake up the actual fiber.
        *lock = State::ResumeWith(result.replace(RunResult::Executing));
        my_state.cond.notify_one();

        // Wait for the fiber to finish
        lock = my_state
            .cond
            .wait_while(lock, |l| !matches!(l, State::SuspendWith(_)))
            .unwrap();

        // Swap the state in our `lock` back into `result`.
        let message = match mem::replace(&mut *lock, State::None) {
            State::SuspendWith(msg) => msg,
            _ => unreachable!(),
        };
        result.set(message);
    }

    unsafe fn state<A, B, C>(&self) -> &SharedFiberState<A, B, C> {
        unsafe { &*(self.state as *const SharedFiberState<A, B, C>) }
    }

    pub(crate) unsafe fn drop<A, B, C>(&mut self) {
        let state = unsafe { self.state::<A, B, C>() };

        // Store an indication that we expect the fiber to exit, then wake it up
        // if it's waiting.
        *state.state.lock().unwrap() = State::Exiting;
        state.cond.notify_one();

        // Wait for the child thread to complete.
        let result = self.thread.take().unwrap().join();

        // Clean up our state using the type parameters we know of here.
        unsafe {
            drop(Arc::from_raw(
                self.state.cast::<SharedFiberState<A, B, C>>(),
            ));
        }
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }
}

impl Suspend {
    fn set_result<A, B, C>(&mut self, result: RunResult<A, B, C>) {
        let state = unsafe { self.state() };
        let mut lock = state.state.lock().unwrap();

        // Our fiber state should be empty, and after verifying that store what
        // we are suspending with.
        assert!(matches!(*lock, State::None));
        *lock = State::SuspendWith(result);
        state.cond.notify_one();
    }

    fn block_until_notified<A, B, C>(&mut self) -> State<A, B, C> {
        let state = unsafe { self.state() };
        let mut lock = state.state.lock().unwrap();
        lock = state
            .cond
            .wait_while(lock, |s| {
                !matches!(s, State::ResumeWith(_) | State::Exiting)
            })
            .unwrap();
        mem::replace(&mut *lock, State::None)
    }

    pub(crate) fn switch<A, B, C>(&mut self, result: RunResult<A, B, C>) -> A {
        self.set_result(result);

        // Wait for the resumption to come back, which is returned from this
        // method.
        match self.block_until_notified::<A, B, C>() {
            State::ResumeWith(RunResult::Resuming(a)) => a,
            _ => unreachable!(),
        }
    }

    pub(crate) fn start_exit<A, B, C>(&mut self, result: RunResult<A, B, C>) {
        self.set_result(result);
    }

    unsafe fn state<A, B, C>(&self) -> &SharedFiberState<A, B, C> {
        unsafe { &*(self.state as *const SharedFiberState<A, B, C>) }
    }
}
