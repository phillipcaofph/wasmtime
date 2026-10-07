//! > **⚠️ Warning ⚠️**: this crate is an internal-only crate for the Wasmtime
//! > project and is not intended for general use. APIs are not strictly
//! > reviewed for safety and usage outside of Wasmtime may have bugs. If
//! > you're interested in using this feel free to file an issue on the
//! > Wasmtime repository to start a discussion about doing so, but otherwise
//! > be aware that your usage of this crate is not supported.

#![no_std]

#[cfg(any(feature = "std", unix, windows))]
#[macro_use]
extern crate std;
extern crate alloc;

use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData;
use core::ops::Range;
use wasmtime_environ::error::Error;

cfg_select! {
    not(feature = "std") => {
        mod nostd;
        use nostd as imp;
        mod stackswitch;
    }
    any(miri, wasmtime_thread_fibers) => {
        mod miri;
        use miri as imp;
    }
    windows => {
        mod windows;
        use windows as imp;
    }
    unix => {
        mod unix;
        use unix as imp;
        mod stackswitch;
    }
    _ => {
        mod nostd;
        use nostd as imp;
        mod stackswitch;
    }
}

/// Represents an execution stack to use for a fiber.
pub struct FiberStack(imp::FiberStack);

impl core::fmt::Debug for FiberStack {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FiberStack").finish_non_exhaustive()
    }
}

fn _assert_send_sync() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    _assert_send::<FiberStack>();
    _assert_sync::<FiberStack>();
}

pub type Result<T, E = imp::Error> = core::result::Result<T, E>;

impl FiberStack {
    /// Creates a new fiber stack of the given size.
    pub fn new(size: usize, zeroed: bool) -> Result<Self> {
        Ok(Self(imp::FiberStack::new(size, zeroed)?))
    }

    /// Creates a new fiber stack of the given size.
    pub fn from_custom(custom: Box<dyn RuntimeFiberStack>) -> Result<Self> {
        Ok(Self(imp::FiberStack::from_custom(custom)?))
    }

    /// Creates a new fiber stack with the given pointer to the bottom of the
    /// stack plus how large the guard size and stack size are.
    ///
    /// The bytes from `bottom` to `bottom.add(guard_size)` should all be
    /// guaranteed to be unmapped. The bytes from `bottom.add(guard_size)` to
    /// `bottom.add(guard_size + len)` should be addressable.
    ///
    /// # Safety
    ///
    /// This is unsafe because there is no validation of the given pointer.
    ///
    /// The caller must properly allocate the stack space with a guard page and
    /// make the pages accessible for correct behavior.
    pub unsafe fn from_raw_parts(bottom: *mut u8, guard_size: usize, len: usize) -> Result<Self> {
        Ok(Self(unsafe {
            imp::FiberStack::from_raw_parts(bottom, guard_size, len)?
        }))
    }

    /// Gets the top of the stack.
    ///
    /// Returns `None` if the platform does not support getting the top of the
    /// stack.
    pub fn top(&self) -> Option<*mut u8> {
        self.0.top()
    }

    /// Returns the range of where this stack resides in memory if the platform
    /// supports it.
    pub fn range(&self) -> Option<Range<usize>> {
        self.0.range()
    }

    /// Is this a manually-managed stack created from raw parts? If so, it is up
    /// to whoever created it to manage the stack's memory allocation.
    pub fn is_from_raw_parts(&self) -> bool {
        self.0.is_from_raw_parts()
    }

    /// Returns the range of memory that the guard page(s) reside in.
    pub fn guard_range(&self) -> Option<Range<*mut u8>> {
        self.0.guard_range()
    }
}

/// A creator of RuntimeFiberStacks.
pub unsafe trait RuntimeFiberStackCreator: Send + Sync {
    /// Creates a new RuntimeFiberStack with the specified size, guard pages should be included,
    /// memory should be zeroed.
    ///
    /// This is useful to plugin previously allocated memory instead of mmap'ing a new stack for
    /// every instance.
    fn new_stack(&self, size: usize, zeroed: bool) -> Result<Box<dyn RuntimeFiberStack>, Error>;
}

/// A fiber stack backed by custom memory.
pub unsafe trait RuntimeFiberStack: Send + Sync {
    /// The top of the allocated stack.
    fn top(&self) -> *mut u8;
    /// The valid range of the stack without guard pages.
    fn range(&self) -> Range<usize>;
    /// The range of the guard page(s)
    fn guard_range(&self) -> Range<*mut u8>;
}

/// A resumable computation with a backend-managed execution context.
///
/// Stack-switching backends run the computation on a switched stack. The
/// experimental thread-backed backend instead gives each fiber a dedicated OS
/// worker thread, which is parked while the fiber is suspended.
pub struct Fiber<'a, Resume, Yield, Return> {
    stack: Option<FiberStack>,
    inner: imp::Fiber,
    done: Cell<bool>,
    #[cfg(wasmtime_thread_fibers)]
    started: Cell<bool>,
    _phantom: PhantomData<&'a (Resume, Yield, Return)>,
}

/// The fiber-side handle used to yield a value to the resumer and receive a
/// value when resumed.
///
/// Stack-switching backends switch back to the resumer's stack when yielding;
/// the experimental thread-backed backend parks the fiber's worker thread
/// until its next resume.
pub struct Suspend<Resume, Yield, Return> {
    inner: imp::Suspend,
    _phantom: PhantomData<(Resume, Yield, Return)>,
}

/// A structure that is stored on a stack frame of a call to `Fiber::resume`.
///
/// This is used to both transmit data to a fiber (the resume step) as well as
/// acquire data from a fiber (the suspension step).
enum RunResult<Resume, Yield, Return> {
    /// The fiber is currently executing meaning it picked up whatever it was
    /// resuming with and hasn't yet completed.
    Executing,

    /// Resume with this value. Called for each invocation of
    /// `Fiber::resume`.
    Resuming(Resume),

    /// The fiber hasn't finished but has provided the following value
    /// during its suspension.
    Yield(Yield),

    /// The fiber has completed with the provided value and can no
    /// longer be resumed.
    Returned(Return),

    /// The fiber execution panicked.
    #[cfg(feature = "std")]
    Panicked(Box<dyn core::any::Any + Send>),
}

impl<'a, Resume, Yield, Return> Fiber<'a, Resume, Yield, Return> {
    /// Creates a new fiber which will execute `func` on the given stack.
    ///
    /// This function returns a `Fiber` which, when resumed, will execute `func`
    /// to completion. When desired the `func` can suspend itself via
    /// `Fiber::suspend`.
    /// On error the provided `stack` is handed back to the caller (paired with
    /// the error) so that it can be deallocated rather than leaked; the stack is
    /// only consumed by this `Fiber` on success.
    #[cfg(not(wasmtime_thread_fibers))]
    pub fn new(
        stack: FiberStack,
        func: impl FnOnce(Resume, &mut Suspend<Resume, Yield, Return>) -> Return + 'a,
    ) -> Result<Self, (Error, FiberStack)> {
        let inner = match imp::Fiber::new(&stack.0, func) {
            Ok(inner) => inner,
            Err(e) => return Err((e, stack)),
        };

        Ok(Self {
            stack: Some(stack),
            inner,
            done: Cell::new(false),
            #[cfg(wasmtime_thread_fibers)]
            started: Cell::new(false),
            _phantom: PhantomData,
        })
    }

    /// Creates an owned thread-backed fiber with compiler-checked transfers.
    ///
    /// ```compile_fail
    /// use std::rc::Rc;
    /// use wasmtime_internal_fiber::{Fiber, FiberStack};
    /// let state = Rc::new(());
    /// let _ = Fiber::<(), (), ()>::new(FiberStack::new(1024 * 1024, false).unwrap(),
    ///     move |_, _| drop(state));
    /// ```
    ///
    /// ```compile_fail
    /// use std::rc::Rc;
    /// use wasmtime_internal_fiber::{Fiber, FiberStack};
    /// let _ = Fiber::<Rc<()>, (), ()>::new(
    ///     FiberStack::new(1024 * 1024, false).unwrap(), |_, _| ());
    /// ```
    ///
    /// ```compile_fail
    /// use wasmtime_internal_fiber::{Fiber, FiberStack};
    /// let mut value = 0;
    /// let _ = Fiber::<(), (), ()>::new(FiberStack::new(1024 * 1024, false).unwrap(),
    ///     |_, _| value += 1);
    /// ```
    #[cfg(wasmtime_thread_fibers)]
    pub fn new(
        stack: FiberStack,
        func: impl FnOnce(Resume, &mut Suspend<Resume, Yield, Return>) -> Return + Send + 'static,
    ) -> Result<Self, (Error, FiberStack)>
    where
        Resume: Send + 'static,
        Yield: Send + 'static,
        Return: Send + 'static,
    {
        unsafe { Self::new_unchecked(stack, func) }
    }

    /// Creates a borrowed thread-backed fiber for Wasmtime's internal scheduler.
    ///
    /// # Safety
    ///
    /// All captures and message types must be safe to transfer to the worker.
    /// Borrowed data must remain valid until the fiber finishes and joins; it
    /// must not be accessed by the caller while the worker has exclusive use.
    /// A suspended stack must not retain references into caller TLS or Context.
    #[cfg(wasmtime_thread_fibers)]
    pub unsafe fn new_unchecked(
        stack: FiberStack,
        func: impl FnOnce(Resume, &mut Suspend<Resume, Yield, Return>) -> Return + 'a,
    ) -> Result<Self, (Error, FiberStack)>
    where
        Resume: Send,
        Yield: Send,
        Return: Send,
    {
        let inner = match imp::Fiber::new(&stack.0, func) {
            Ok(inner) => inner,
            Err(e) => return Err((e, stack)),
        };
        Ok(Self {
            stack: Some(stack),
            inner,
            done: Cell::new(false),
            started: Cell::new(false),
            _phantom: PhantomData,
        })
    }

    /// Resumes execution of this fiber.
    ///
    /// This function will transfer execution to the fiber and resume from where
    /// it last left off.
    ///
    /// Returns `true` if the fiber finished or `false` if the fiber was
    /// suspended in the middle of execution.
    ///
    /// # Panics
    ///
    /// Panics if this fiber has already finished.
    ///
    /// Note that if the fiber itself panics during execution then the panic
    /// will be propagated to this caller.
    pub fn resume(&self, val: Resume) -> Result<Return, Yield> {
        #[cfg(wasmtime_thread_fibers)]
        self.started.set(true);
        assert!(!self.done.replace(true), "cannot resume a finished fiber");
        let result = Cell::new(RunResult::Resuming(val));
        self.inner.resume(&self.stack().0, &result);
        match result.into_inner() {
            RunResult::Resuming(_) | RunResult::Executing => unreachable!(),
            RunResult::Yield(y) => {
                self.done.set(false);
                Err(y)
            }
            RunResult::Returned(r) => Ok(r),
            #[cfg(feature = "std")]
            RunResult::Panicked(_payload) => {
                use std::panic;
                panic::resume_unwind(_payload);
            }
        }
    }

    /// Returns whether this fiber has finished executing.
    pub fn done(&self) -> bool {
        self.done.get()
    }

    /// Gets the stack associated with this fiber.
    pub fn stack(&self) -> &FiberStack {
        self.stack.as_ref().unwrap()
    }

    /// When this fiber has finished executing, reclaim its stack.
    pub fn into_stack(mut self) -> FiberStack {
        assert!(self.done());
        self.stack.take().unwrap()
    }
}

impl<Resume, Yield, Return> Suspend<Resume, Yield, Return> {
    /// Suspend execution of a currently running fiber.
    ///
    /// This function will switch control back to the original caller of
    /// `Fiber::resume`. This function will then return once the `Fiber::resume`
    /// function is called again.
    ///
    /// # Panics
    ///
    /// Panics if the current thread is not executing a fiber from this library.
    pub fn suspend(&mut self, value: Yield) -> Resume {
        self.inner
            .switch::<Resume, Yield, Return>(RunResult::Yield(value))
    }

    fn execute(
        inner: imp::Suspend,
        initial: Resume,
        func: impl FnOnce(Resume, &mut Suspend<Resume, Yield, Return>) -> Return,
    ) -> imp::Suspend {
        let mut suspend = Suspend {
            inner,
            _phantom: PhantomData,
        };

        #[cfg(feature = "std")]
        let result = {
            use std::panic::{self, AssertUnwindSafe};
            let result = panic::catch_unwind(AssertUnwindSafe(|| (func)(initial, &mut suspend)));
            match result {
                Ok(result) => RunResult::Returned(result),
                Err(panic) => RunResult::Panicked(panic),
            }
        };

        // Note that it is sound to omit the `catch_unwind` here: it
        // will not result in unwinding going off the top of the fiber
        // stack, because the code on the fiber stack is invoked via
        // an extern "C" boundary which will panic on unwinds.
        #[cfg(not(feature = "std"))]
        let result = RunResult::Returned((func)(initial, &mut suspend));

        suspend.inner.start_exit::<Resume, Yield, Return>(result);
        suspend.inner
    }
}

impl<A, B, C> Drop for Fiber<'_, A, B, C> {
    fn drop(&mut self) {
        #[cfg(not(wasmtime_thread_fibers))]
        debug_assert!(self.done.get(), "fiber dropped without finishing");
        #[cfg(wasmtime_thread_fibers)]
        debug_assert!(
            self.done.get() || !self.started.get(),
            "fiber dropped while suspended"
        );
        unsafe {
            self.inner.drop::<A, B, C>();
        }
    }
}

#[cfg(all(test))]
mod tests {
    use super::{Fiber, FiberStack};
    use alloc::string::ToString;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Flag(AtomicBool);

    impl Flag {
        fn new(value: bool) -> Self {
            Self(AtomicBool::new(value))
        }
        fn get(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
        fn set(&self, value: bool) {
            self.0.store(value, Ordering::SeqCst);
        }
    }

    fn fiber_stack(size: usize) -> FiberStack {
        FiberStack::new(size, false).unwrap()
    }

    #[test]
    fn small_stacks() {
        Fiber::<(), (), ()>::new(fiber_stack(0), |_, _| {})
            .unwrap()
            .resume(())
            .unwrap();
        Fiber::<(), (), ()>::new(fiber_stack(1), |_, _| {})
            .unwrap()
            .resume(())
            .unwrap();
    }

    #[cfg(wasmtime_thread_fibers)]
    #[test]
    fn unstarted_worker_is_joined_and_capture_dropped() {
        struct Capture(Arc<Flag>);
        impl Drop for Capture {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let dropped = Arc::new(Flag::new(false));
        let capture = Capture(dropped.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        let parent = std::thread::spawn(move || {
            let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |_, _| {
                let _ = &capture;
                panic!("unstarted fiber ran its body");
            })
            .unwrap();
            drop(fiber);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("dropping an unstarted worker deadlocked");
        parent.join().unwrap();
        assert!(dropped.get());
    }

    #[cfg(wasmtime_thread_fibers)]
    #[test]
    fn unstarted_worker_capture_drop_panic_is_propagated_after_join() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        struct PanickingCapture(Arc<Flag>);
        impl Drop for PanickingCapture {
            fn drop(&mut self) {
                self.0.set(true);
                std::panic::panic_any("unstarted worker capture drop");
            }
        }

        let dropped = Arc::new(Flag::new(false));
        let ran = Arc::new(Flag::new(false));
        let capture = PanickingCapture(dropped.clone());
        let ran_body = ran.clone();
        let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |_, _| {
            ran_body.set(true);
            drop(capture);
        })
        .unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| drop(fiber)))
            .expect_err("worker capture destructor panic was swallowed");
        assert_eq!(
            panic.downcast_ref::<&'static str>(),
            Some(&"unstarted worker capture drop"),
            "worker panic payload was not preserved across join"
        );
        assert!(dropped.get(), "worker capture was not destroyed");
        assert!(!ran.get(), "unstarted worker ran its body");
    }

    #[cfg(wasmtime_thread_fibers)]
    #[test]
    fn failed_worker_spawn_returns_stack_and_drops_capture() {
        struct Capture(Arc<Flag>);
        impl Drop for Capture {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let dropped = Arc::new(Flag::new(false));
        let ran = Arc::new(Flag::new(false));
        let capture = Capture(dropped.clone());
        let ran_body = ran.clone();
        let stack = fiber_stack(1024 * 1024);
        let expected_size = super::imp::stack_size_for_test(&stack.0);

        super::imp::fail_next_spawn_for_test();
        let (error, returned_stack) = match Fiber::<(), (), ()>::new(stack, move |_, _| {
            ran_body.set(true);
            drop(capture);
        }) {
            Err(error_and_stack) => error_and_stack,
            Ok(_) => panic!("injected thread creation failure was ignored"),
        };

        assert!(
            error.to_string().contains("injected worker spawn failure"),
            "unexpected worker creation error: {error}"
        );
        assert_eq!(
            super::imp::stack_size_for_test(&returned_stack.0),
            expected_size
        );
        assert!(dropped.get(), "failed spawn leaked the closure capture");
        assert!(!ran.get(), "failed worker ran its body");
    }

    #[test]
    fn smoke() {
        let hit = Arc::new(Flag::new(false));
        let hit2 = hit.clone();
        let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |_, _| {
            hit2.set(true);
        })
        .unwrap();
        assert!(!hit.get());
        fiber.resume(()).unwrap();
        assert!(hit.get());
    }

    #[test]
    fn suspend_and_resume() {
        let hit = Arc::new(Flag::new(false));
        let hit2 = hit.clone();
        let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |_, s| {
            s.suspend(());
            hit2.set(true);
            s.suspend(());
        })
        .unwrap();
        assert!(!hit.get());
        assert!(fiber.resume(()).is_err());
        assert!(!hit.get());
        assert!(fiber.resume(()).is_err());
        assert!(hit.get());
        assert!(fiber.resume(()).is_ok());
        assert!(hit.get());
    }

    #[test]
    #[cfg(not(wasmtime_thread_fibers))]
    fn backtrace_traces_to_host() {
        #[inline(never)] // try to get this to show up in backtraces
        fn look_for_me() {
            run_test();
        }
        fn assert_contains_host() {
            let trace = backtrace::Backtrace::new();
            println!("{trace:?}");
            assert!(
                trace
                .frames()
                .iter()
                .flat_map(|f| f.symbols())
                .filter_map(|s| Some(s.name()?.to_string()))
                .any(|s| s.contains("look_for_me"))
                // TODO: apparently windows unwind routines don't unwind through fibers, so this will always fail. Is there a way we can fix that?
                || cfg!(windows)
                // TODO: the system libunwind is broken (#2808)
                || cfg!(all(target_os = "macos", target_arch = "aarch64"))
                // TODO: see comments in `arm.rs` about how this seems to work
                // in gdb but not at runtime, unsure why at this time.
                || cfg!(target_arch = "arm")
                // asan does weird things
                || cfg!(asan)
                // miri is a bit of a stretch to get working here
                || cfg!(miri)
            );
        }

        fn run_test() {
            let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |(), s| {
                assert_contains_host();
                s.suspend(());
                assert_contains_host();
                s.suspend(());
                assert_contains_host();
            })
            .unwrap();
            assert!(fiber.resume(()).is_err());
            assert!(fiber.resume(()).is_err());
            assert!(fiber.resume(()).is_ok());
        }

        look_for_me();
    }

    #[test]
    #[cfg(wasmtime_thread_fibers)]
    fn backtrace_traces_worker_not_poller() {
        #[inline(never)]
        fn assert_worker_backtrace() {
            let trace = backtrace::Backtrace::new();
            let contains = |name: &str| {
                trace
                    .frames()
                    .iter()
                    .flat_map(|frame| frame.symbols())
                    .filter_map(|symbol| symbol.name())
                    .any(|symbol| symbol.to_string().contains(name))
            };
            assert!(contains("::worker_frame"), "{trace:?}");
            assert!(!contains("::poller_frame"), "{trace:?}");
        }

        #[inline(never)]
        fn worker_frame(suspend: &mut super::Suspend<(), (), ()>, poller: std::thread::ThreadId) {
            let worker = std::thread::current().id();
            assert_ne!(worker, poller);
            for _ in 0..2 {
                assert_worker_backtrace();
                suspend.suspend(());
                assert_eq!(std::thread::current().id(), worker);
            }
            assert_worker_backtrace();
            std::hint::black_box(worker);
        }

        #[inline(never)]
        fn poller_frame(fiber: &Fiber<'_, (), (), ()>) {
            assert!(fiber.resume(()).is_err());
            assert!(fiber.resume(()).is_err());
            assert!(fiber.resume(()).is_ok());
        }

        let poller = std::thread::current().id();
        let fiber = Fiber::new(fiber_stack(1024 * 1024), move |(), suspend| {
            worker_frame(suspend, poller);
        })
        .unwrap();
        poller_frame(&fiber);
    }

    #[test]
    #[cfg(feature = "std")]
    fn panics_propagated() {
        use std::panic::{self, AssertUnwindSafe};

        let a = Arc::new(Flag::new(false));
        let b = SetOnDrop(a.clone());
        let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |(), _s| {
            let _ = &b;
            panic!();
        })
        .unwrap();
        assert!(panic::catch_unwind(AssertUnwindSafe(|| fiber.resume(()))).is_err());
        assert!(a.get());

        struct SetOnDrop(Arc<Flag>);

        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
    }

    #[test]
    fn suspend_and_resume_values() {
        let fiber = Fiber::new(fiber_stack(1024 * 1024), move |first, s| {
            assert_eq!(first, 2.0);
            assert_eq!(s.suspend(4), 3.0);
            "hello".to_string()
        })
        .unwrap();
        assert_eq!(fiber.resume(2.0), Err(4));
        assert_eq!(fiber.resume(3.0), Ok("hello".to_string()));
    }

    #[test]
    fn fiber_stack_max_size() {
        if cfg!(windows) || cfg!(miri) {
            return;
        }
        assert!(FiberStack::new(usize::MAX, true).is_err());
        assert!(FiberStack::new(usize::MAX, false).is_err());
    }

    #[test]
    // Don't run under `std` -- we assert that the stack is page-aligned there.
    #[cfg(not(feature = "std"))]
    fn custom_stack() {
        use super::RuntimeFiberStack;
        use core::ops::Range;

        struct CustomStack {
            // `u128` to guarantee alignment.
            buf: std::vec::Vec<u128>,
        }
        unsafe impl Send for CustomStack {}
        unsafe impl Sync for CustomStack {}
        unsafe impl RuntimeFiberStack for CustomStack {
            fn top(&self) -> *mut u8 {
                self.range().end as *mut u8
            }
            fn range(&self) -> Range<usize> {
                let base = self.buf.as_ptr() as usize;
                base..base + self.buf.len() * core::mem::size_of::<u128>()
            }
            fn guard_range(&self) -> Range<*mut u8> {
                core::ptr::null_mut()..core::ptr::null_mut()
            }
        }

        // A 1 MiB stack (65536 * 16 bytes).
        let stack = FiberStack::from_custom(std::boxed::Box::new(CustomStack {
            buf: std::vec![0u128; 1024 * 1024 / 16],
        }))
        .unwrap();
        // A custom stack is not considered a `from_raw_parts` stack.
        assert!(!stack.is_from_raw_parts());

        let hit = Arc::new(Flag::new(false));
        let hit2 = hit.clone();
        let fiber = Fiber::<(), (), ()>::new(stack, move |_, s| {
            hit2.set(true);
            s.suspend(());
            hit2.set(false);
        })
        .unwrap();
        assert!(!hit.get());
        // First resume runs up to the suspend point.
        assert!(fiber.resume(()).is_err());
        assert!(hit.get());
        // Second resume runs to completion.
        assert!(fiber.resume(()).is_ok());
        assert!(!hit.get());

        // The reclaimed stack still reports its range correctly.
        let stack = fiber.into_stack();
        assert!(stack.range().is_some());
    }

    #[test]
    fn cross_thread_fiber() {
        let fiber = Fiber::<(), (), ()>::new(fiber_stack(1024 * 1024), move |_, s| {
            s.suspend(());
        })
        .unwrap();
        assert!(fiber.resume(()).is_err());
        let fiber = UnsafeSendSync(fiber);
        std::thread::spawn(move || {
            let fiber = fiber;
            assert!(fiber.0.resume(()).is_ok());
        })
        .join()
        .unwrap();

        struct UnsafeSendSync<T>(T);

        unsafe impl<T> Send for UnsafeSendSync<T> {}
        unsafe impl<T> Sync for UnsafeSendSync<T> {}
    }
}
