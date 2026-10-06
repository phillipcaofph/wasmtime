use super::{Accessor, tls};
use crate::component::{Component, Linker};
use crate::prelude::*;
use crate::{AsContextMut, Collector, Config, Engine, Result, Store, Trap, WasmBacktrace};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread::{self, ThreadId};

pub(super) const COMPONENT: &str = r#"
(component
    (import "wait" (func $wait async (param "value" u32)))
    (import "checked" (func $checked))
    (core module $libc (table (export "table") 1 funcref))
    (core instance $libc (instantiate $libc))
    (core module $m
        (type $root (struct (field i32)))
        (type $garbage (array (mut i8)))
        (import "" "wait" (func $wait (param i32)))
        (import "" "checked" (func $checked))
        (import "" "new" (func $new (param i32 i32) (result i32)))
        (import "" "resume" (func $resume (param i32)))
        (import "" "table" (table $table 1 funcref))
        (func $hold (param $value i32)
            (local $root (ref $root))
            (local.set $root (struct.new $root (local.get $value)))
            (array.new_default $garbage (i32.const 65536))
            drop
            local.get $value
            call $wait
            (struct.get $root 0 (local.get $root))
            local.get $value
            i32.ne
            if unreachable end
            call $checked)
        (func $entry (param i32)
            i32.const 6060
            call $hold)
        (elem (table $table) (i32.const 0) func $entry)
        (func $run (export "run")
            (call $resume (call $new (i32.const 0) (i32.const 0)))
            (call $resume (call $new (i32.const 0) (i32.const 0)))
            i32.const 5050
            call $hold))
    (core type $entry (func (param i32)))
    (core func $new (canon thread.new-indirect $entry (core table $libc "table")))
    (core func $resume (canon thread.resume-later))
    (core func $wait (canon lower (func $wait)))
    (core func $checked (canon lower (func $checked)))
    (core instance $i (instantiate $m
        (with "" (instance
            (export "new" (func $new))
            (export "resume" (func $resume))
            (export "wait" (func $wait))
            (export "checked" (func $checked))
            (export "table" (table $libc "table"))))))
    (func (export "run") async (canon lift (core func $i "run"))))
"#;

#[derive(Default)]
struct State {
    entered: usize,
    checked: usize,
}

#[derive(Default)]
struct Observations {
    release: AtomicBool,
    fail: AtomicBool,
    release_id: AtomicUsize,
    dropped: AtomicUsize,
    polls: Mutex<Vec<(usize, ThreadId)>>,
    values: Mutex<Vec<(usize, u32)>>,
}

struct Waiting<'a> {
    accessor: &'a Accessor<State>,
    observations: Arc<Observations>,
    id: Option<usize>,
    value: u32,
}

impl Future for Waiting<'_> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = match self.id {
            Some(id) => id,
            None => {
                let id = self.accessor.with(|mut access| {
                    let state = access.data_mut();
                    state.entered += 1;
                    state.entered
                });
                self.id = Some(id);
                self.observations
                    .values
                    .lock()
                    .unwrap()
                    .push((id, self.value));
                id
            }
        };
        self.accessor.with(|mut access| {
            assert!(access.data_mut().entered >= id);
            tls::try_get(|state| assert!(matches!(state, tls::TryGet::Taken)));
        });
        self.observations
            .polls
            .lock()
            .unwrap()
            .push((id, thread::current().id()));
        let release_id = self.observations.release_id.load(Ordering::SeqCst);
        if self.observations.release.load(Ordering::SeqCst) && (release_id == 0 || release_id == id)
        {
            Poll::Ready(if self.observations.fail.load(Ordering::SeqCst) {
                Err(crate::format_err!("parked host task failed"))
            } else {
                Ok(())
            })
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        // Accessor use in arbitrary Drop is not a public guarantee. Only check
        // destruction, leaving the TLS checks in the supported poll scope.
        self.observations.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn assert_tls_empty() {
    tls::try_get(|state| assert!(matches!(state, tls::TryGet::None)));
    crate::vm::tls::with(|state| assert!(state.is_none()));
}

fn engine(collector: Collector) -> Result<Engine> {
    let mut config = Config::new();
    config.macos_use_mach_ports(false);
    config.wasm_component_model_async(true);
    config.wasm_component_model_more_async_builtins(true);
    config.wasm_component_model_async_stackful(true);
    config.wasm_component_model_threading(true);
    config.collector(collector);
    Engine::new(&config)
}

#[derive(Clone, Copy)]
enum Finish {
    Complete,
    Cancel,
    HostError,
    RootTrap,
    ChildTrap,
}

fn run(collector: Collector, finish: Finish) -> Result<()> {
    assert_tls_empty();
    let engine = engine(collector)?;
    let source = match finish {
        Finish::RootTrap => COMPONENT.replace(
            "call $checked)",
            "local.get $value i32.const 5050 i32.eq if unreachable end call $checked)",
        ),
        Finish::ChildTrap => COMPONENT.replace(
            "call $checked)",
            "local.get $value i32.const 6060 i32.eq if unreachable end call $checked)",
        ),
        _ => COMPONENT.to_string(),
    };
    let component = Component::new(&engine, source)?;
    let mut store = Store::new(&engine, State::default());
    let observations = Arc::new(Observations::default());
    let observed = observations.clone();
    let mut linker = Linker::<State>::new(&engine);
    linker
        .root()
        .func_wrap_concurrent("wait", move |accessor, (value,): (u32,)| {
            Box::pin(Waiting {
                accessor,
                observations: observed.clone(),
                id: None,
                value,
            })
        })?;
    linker.root().func_wrap("checked", |mut store, ()| {
        store.data_mut().checked += 1;
        Ok(())
    })?;
    let instance = {
        let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
        match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result?,
            Poll::Pending => panic!("fixture instantiation unexpectedly suspended"),
        }
    };
    let func = instance.get_typed_func::<(), ()>(&mut store, "run")?;
    let before = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    let collections = Arc::new(AtomicUsize::new(0));
    let collected = collections.clone();
    let mut future = Box::pin(store.run_concurrent(async |accessor| -> Result<()> {
        let mut call = core::pin::pin!(func.call_concurrent(accessor, ()));
        core::future::poll_fn(|cx| {
            if let Poll::Ready(result) = call.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
            if !observations.release.load(Ordering::SeqCst)
                && accessor.with(|mut access| access.data_mut().entered) == 3
            {
                for _ in 0..3 {
                    accessor.with(|mut access| {
                        assert!(
                            access.as_context_mut().0.parked_wasm_stack_root_count() >= 3,
                            "GC tracing must find roots on all three parked guest stacks"
                        );
                    });
                    accessor.with(|mut access| access.as_context_mut().gc(None))?;
                    collected.fetch_add(1, Ordering::SeqCst);
                }
            }
            Poll::Pending
        })
        .await?;
        core::future::poll_fn(|cx| {
            if accessor.with(|mut access| access.data_mut().checked) == 3 {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        Ok(())
    }));
    let first = thread::current().id();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_tls_empty();
        let polls = observations.polls.lock().unwrap();
        if (1..=3).all(|id| polls.contains(&(id, first))) && collections.load(Ordering::SeqCst) >= 3
        {
            break;
        }
    }
    assert_eq!(
        observations
            .polls
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| *id)
            .max(),
        Some(3),
        "all three same-Store guest threads must park"
    );
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() >= before + 3);

    // Polling a concurrent root future lets us collect while all guest stacks
    // remain suspended, without retaining a Store borrow across the handoff.
    let second = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                for _ in 0..100 {
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    assert_tls_empty();
                    let polls = observations.polls.lock().unwrap();
                    if (1..=3).all(|id| polls.contains(&(id, thread::current().id()))) {
                        break;
                    }
                }
                thread::current().id()
            })
            .join()
            .unwrap()
    });
    assert_ne!(first, second);
    assert!(collections.load(Ordering::SeqCst) >= 6);
    {
        let polls = observations.polls.lock().unwrap();
        let workers: Vec<_> = (1..=3)
            .map(|id| {
                let worker = polls.iter().find(|(task, _)| *task == id).unwrap().1;
                assert_ne!(worker, first);
                assert_ne!(worker, second);
                assert!(
                    polls.contains(&(id, first)),
                    "host task did not reach first scheduler"
                );
                assert!(polls.contains(&(id, second)), "host task did not migrate");
                worker
            })
            .collect();
        assert_ne!(workers[0], workers[1]);
        assert_ne!(workers[0], workers[2]);
        assert_ne!(workers[1], workers[2]);
    }
    if !matches!(finish, Finish::Cancel) {
        if matches!(finish, Finish::RootTrap | Finish::ChildTrap) {
            let value = if matches!(finish, Finish::RootTrap) {
                5050
            } else {
                6060
            };
            let id = observations
                .values
                .lock()
                .unwrap()
                .iter()
                .find(|(_, v)| *v == value)
                .unwrap()
                .0;
            observations.release_id.store(id, Ordering::SeqCst);
        }
        observations
            .fail
            .store(matches!(finish, Finish::HostError), Ordering::SeqCst);
        observations.release.store(true, Ordering::SeqCst);
        let mut complete = false;
        for _ in 0..100 {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(result) => {
                    match finish {
                        Finish::Complete => result??,
                        Finish::HostError => {
                            let error = match result {
                                Err(error) | Ok(Err(error)) => error,
                                Ok(Ok(())) => panic!("host error was lost"),
                            };
                            assert!(format!("{error:#}").contains("parked host task failed"));
                        }
                        Finish::RootTrap | Finish::ChildTrap => {
                            let error = match result {
                                Err(error) | Ok(Err(error)) => error,
                                Ok(Ok(())) => panic!("guest trap was lost"),
                            };
                            assert_eq!(
                                error.downcast_ref::<Trap>(),
                                Some(&Trap::UnreachableCodeReached)
                            );
                            let trace = error
                                .downcast_ref::<WasmBacktrace>()
                                .expect("guest trap must include a Wasm backtrace");
                            assert!(
                                trace
                                    .frames()
                                    .iter()
                                    .any(|frame| frame.func_name() == Some("hold"))
                            );
                            let entry = if matches!(finish, Finish::RootTrap) {
                                "run"
                            } else {
                                "entry"
                            };
                            assert!(
                                trace
                                    .frames()
                                    .iter()
                                    .any(|frame| frame.func_name() == Some(entry)),
                                "trap backtrace did not identify the selected guest thread"
                            );
                            assert_eq!(
                                observations.dropped.load(Ordering::SeqCst),
                                1,
                                "only the trapping worker's host future should have completed"
                            );
                        }
                        Finish::Cancel => unreachable!(),
                    }
                    complete = true;
                    break;
                }
                Poll::Pending => assert_tls_empty(),
            }
        }
        assert!(complete);
    }
    drop(future);
    if matches!(finish, Finish::RootTrap | Finish::ChildTrap) {
        assert_eq!(store.data().checked, 0);
    }
    if matches!(finish, Finish::Complete) {
        assert_eq!(store.data().checked, 3);
    }
    drop(store);
    assert_eq!(observations.dropped.load(Ordering::SeqCst), 3);
    assert_tls_empty();
    Ok(())
}

#[test]
fn accessor_tls_same_store_parked_roots_drc() -> Result<()> {
    run(Collector::DeferredReferenceCounting, Finish::Complete)
}

#[test]
fn accessor_tls_same_store_parked_roots_null() -> Result<()> {
    run(Collector::Null, Finish::Complete)
}

#[test]
fn accessor_tls_same_store_cancellation() -> Result<()> {
    run(Collector::DeferredReferenceCounting, Finish::Cancel)
}

#[test]
fn accessor_tls_same_store_host_error() -> Result<()> {
    run(Collector::DeferredReferenceCounting, Finish::HostError)
}

#[test]
fn cross_fiber_root_trap_with_parked_siblings_drc() -> Result<()> {
    isolated_trap(
        "cross_fiber_root_trap_with_parked_siblings_drc",
        Collector::DeferredReferenceCounting,
        Finish::RootTrap,
    )
}

#[test]
fn cross_fiber_child_trap_with_parked_siblings_drc() -> Result<()> {
    isolated_trap(
        "cross_fiber_child_trap_with_parked_siblings_drc",
        Collector::DeferredReferenceCounting,
        Finish::ChildTrap,
    )
}

#[cfg(feature = "gc-copying")]
#[test]
fn cross_fiber_root_trap_with_parked_siblings_copying() -> Result<()> {
    isolated_trap(
        "cross_fiber_root_trap_with_parked_siblings_copying",
        Collector::Copying,
        Finish::RootTrap,
    )
}

#[cfg(feature = "gc-copying")]
#[test]
fn cross_fiber_child_trap_with_parked_siblings_copying() -> Result<()> {
    isolated_trap(
        "cross_fiber_child_trap_with_parked_siblings_copying",
        Collector::Copying,
        Finish::ChildTrap,
    )
}

fn isolated_trap(name: &str, collector: Collector, finish: Finish) -> Result<()> {
    isolated_test(
        &format!("runtime::component::concurrent::thread_tests::{name}"),
        || {
            run(collector, finish)?;
            assert_workers_stopped();
            // A new healthy Store on the same polling thread must still work.
            run(collector, Finish::Complete)
        },
        6,
    )
}

pub(super) fn assert_workers_stopped() {
    unsafe extern "C" {
        fn wasmtime_thread_fiber_live() -> u64;
    }
    // The diagnostic has no pointer arguments and is only read in isolated tests.
    assert_eq!(
        unsafe { wasmtime_thread_fiber_live() },
        0,
        "execution worker leaked"
    );
}

pub(super) fn workers_started() -> u64 {
    unsafe extern "C" {
        fn wasmtime_thread_fiber_started() -> u64;
    }
    // The diagnostic has no pointer arguments and is only read in isolated tests.
    unsafe { wasmtime_thread_fiber_started() }
}

pub(super) fn isolated_test(
    test: &str,
    run: impl FnOnce() -> Result<()>,
    workers: u64,
) -> Result<()> {
    const CHILD: &str = "WASMTIME_THREAD_TRAP_TEST_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        unsafe extern "C" {
            fn wasmtime_thread_fiber_started() -> u64;
        }
        // These experiment-only diagnostics have no pointer arguments.
        assert_workers_stopped();
        let before = unsafe { wasmtime_thread_fiber_started() };
        run()?;
        assert!(unsafe { wasmtime_thread_fiber_started() } >= before + workers);
        assert_workers_stopped();
        return Ok(());
    }
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD, test)
        .spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait()? {
            ensure!(
                status.success(),
                "isolated thread test {test} failed: {status}"
            );
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            bail!("isolated thread test {test} timed out");
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "gc-copying")]
#[test]
fn accessor_tls_same_store_parked_roots_copying() -> Result<()> {
    run(Collector::Copying, Finish::Complete)
}

#[test]
fn accessor_tls_nested_scopes_restore_taken_context_after_unwind() -> Result<()> {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    assert_tls_empty();
    let engine = engine(Collector::DeferredReferenceCounting)?;
    let mut outer = Store::new(&engine, 101usize);
    let mut inner = Store::new(&engine, 202usize);
    let inner_accessor = Accessor::new(crate::store::StoreToken::new(inner.as_context_mut()));
    let mut future = Box::pin(outer.run_concurrent(async |accessor| {
        accessor.with(|mut access| {
            assert_eq!(*access.data_mut(), 101);
            {
                tls::set(inner.as_context_mut().0, || {
                    inner_accessor.with(|mut access| assert_eq!(*access.data_mut(), 202));
                });
            }
            tls::try_get(|state| assert!(matches!(state, tls::TryGet::Taken)));
            let caught = catch_unwind(AssertUnwindSafe(|| {
                tls::set(inner.as_context_mut().0, || {
                    inner_accessor.with(|mut access| assert_eq!(*access.data_mut(), 202));
                    panic!("intentional nested poll panic");
                });
            }));
            let panic = caught.expect_err("nested scope did not panic");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"intentional nested poll panic")
            );
            tls::try_get(|state| assert!(matches!(state, tls::TryGet::Taken)));
            {
                let mut nested = Box::pin(inner.run_concurrent(async |_| ()));
                assert!(matches!(
                    nested
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Ok(()))
                ));
            }
            tls::try_get(|state| assert!(matches!(state, tls::TryGet::Taken)));
        });
        accessor.with(|mut access| assert_eq!(*access.data_mut(), 101));
    }));
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop(future);
    assert_tls_empty();
    Ok(())
}
