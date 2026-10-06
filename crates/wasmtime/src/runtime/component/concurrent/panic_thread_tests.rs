use super::thread_tests::{
    assert_tls_empty, assert_workers_stopped, isolated_test, workers_started,
};
use crate::component::{Component, Linker, Resource, ResourceType};
use crate::prelude::*;
use crate::{Config, Engine, Func, Instance, Module, Store};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::{self, ThreadId};

use super::thread_tests::COMPONENT as THREE_GUEST_COMPONENT;

#[derive(Default)]
struct Observations {
    polls: Mutex<Vec<ThreadId>>,
    dropped: AtomicUsize,
    payload_dropped: AtomicUsize,
    drops_on: Mutex<Vec<ThreadId>>,
}

struct Payload {
    marker: u32,
    origin: ThreadId,
    observations: Arc<Observations>,
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.observations
            .payload_dropped
            .fetch_add(1, Ordering::SeqCst);
    }
}

fn raise(observations: &Arc<Observations>) -> ! {
    panic_any(Payload {
        marker: 5050,
        origin: thread::current().id(),
        observations: observations.clone(),
    })
}

fn check_panic(action: impl FnOnce(), observations: &Observations, origin: ThreadId) {
    let payload = catch_unwind(AssertUnwindSafe(action)).expect_err("host panic was lost");
    assert_tls_empty();
    let payload = payload
        .downcast::<Payload>()
        .expect("panic payload changed");
    assert_eq!(payload.marker, 5050);
    assert_eq!(payload.origin, origin);
    assert_eq!(observations.payload_dropped.load(Ordering::SeqCst), 0);
    drop(payload);
    assert_eq!(observations.payload_dropped.load(Ordering::SeqCst), 1);
}

struct PendingPanic {
    observations: Arc<Observations>,
    polls: usize,
    panic_poll: usize,
    panic_drop: bool,
}

impl Future for PendingPanic {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls += 1;
        self.observations
            .polls
            .lock()
            .unwrap()
            .push(thread::current().id());
        if self.polls == self.panic_poll {
            raise(&self.observations);
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for PendingPanic {
    fn drop(&mut self) {
        self.observations.dropped.fetch_add(1, Ordering::SeqCst);
        self.observations
            .drops_on
            .lock()
            .unwrap()
            .push(thread::current().id());
        if self.panic_drop {
            raise(&self.observations);
        }
    }
}

fn engine() -> Result<Engine> {
    let mut config = Config::new();
    config.macos_use_mach_ports(false);
    config.wasm_component_model_async(true);
    Engine::new(&config)
}

fn ready<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(r) => r,
        Poll::Pending => panic!("fixture initialization unexpectedly suspended"),
    }
}

fn healthy(engine: &Engine) -> Result<()> {
    assert_tls_empty();
    let module = Module::new(
        engine,
        "(module (func (export \"run\") (result i32) i32.const 42))",
    )?;
    let mut store = Store::new(engine, ());
    let instance = ready(Instance::new_async(&mut store, &module, &[]))?;
    let func = instance.get_typed_func::<(), i32>(&mut store, "run")?;
    assert_eq!(ready(func.call_async(&mut store, ()))?, 42);
    drop(store);
    assert_tls_empty();
    assert_workers_stopped();
    Ok(())
}

fn migrate_pending<F: Future + Send>(mut future: Pin<&mut F>) -> ThreadId {
    let suspensions = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_tls_empty();
    let migrated = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert_tls_empty();
                thread::current().id()
            })
            .join()
            .unwrap()
    });
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() >= suspensions + 2);
    migrated
}

fn core_panic(panic_poll: usize, panic_drop: bool) -> Result<()> {
    let engine = engine()?;
    let mut store = Store::new(&engine, ());
    let observations = Arc::new(Observations::default());
    let observed = observations.clone();
    let host = if panic_poll == 0 && !panic_drop {
        Func::wrap(&mut store, move || -> () {
            observed.polls.lock().unwrap().push(thread::current().id());
            raise(&observed);
        })
    } else {
        Func::wrap_async(&mut store, move |_, (): ()| {
            Box::new(PendingPanic {
                observations: observed.clone(),
                polls: 0,
                panic_poll,
                panic_drop,
            })
        })
    };
    let module = Module::new(
        &engine,
        "(module (import \"\" \"host\" (func $host)) (func $run (export \"run\") call $host))",
    )?;
    let instance = ready(Instance::new_async(&mut store, &module, &[host.into()]))?;
    let func = instance.get_typed_func::<(), ()>(&mut store, "run")?;
    let mut future = Box::pin(func.call_async(&mut store, ()));
    let first = thread::current().id();
    if panic_poll > 1 || panic_drop {
        let second = migrate_pending(future.as_mut());
        assert_ne!(first, second);
    }
    // Immediate panics have not recorded their worker yet.
    let payload = catch_unwind(AssertUnwindSafe(|| {
        if panic_drop {
            drop(future);
        } else {
            let result = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            assert!(result.is_ready(), "panic poll unexpectedly suspended");
            drop(future);
        }
    }))
    .expect_err("worker panic was lost");
    assert_tls_empty();
    let origin = observations.polls.lock().unwrap()[0];
    assert_ne!(origin, first);
    check_panic(|| std::panic::resume_unwind(payload), &observations, origin);
    assert_eq!(
        observations.dropped.load(Ordering::SeqCst),
        usize::from(panic_poll != 0 || panic_drop)
    );
    assert!(
        observations
            .drops_on
            .lock()
            .unwrap()
            .iter()
            .all(|id| *id == origin)
    );
    drop(store);
    assert_workers_stopped();
    healthy(&engine)
}

fn concurrent_panic(panic_poll: usize, panic_drop: bool) -> Result<()> {
    let engine = engine()?;
    let component = Component::new(
        &engine,
        r#"
        (component
            (import "host" (func $host async))
            (core func $host (canon lower (func $host)))
            (core module $m
                (import "" "host" (func $host))
                (func $run (export "run") call $host))
            (core instance $i (instantiate $m
                (with "" (instance (export "host" (func $host))))))
            (func (export "run") async (canon lift (core func $i "run"))))
    "#,
    )?;
    let observations = Arc::new(Observations::default());
    let observed = observations.clone();
    let mut linker = Linker::<u32>::new(&engine);
    linker
        .root()
        .func_wrap_concurrent("host", move |accessor, (): ()| {
            let mut pending = Box::pin(PendingPanic {
                observations: observed.clone(),
                polls: 0,
                panic_poll,
                panic_drop,
            });
            Box::pin(async move {
                core::future::poll_fn(|cx| {
                    accessor.with(|mut access| {
                        assert_eq!(*access.data_mut(), 101);
                        super::tls::try_get(|state| {
                            assert!(matches!(state, super::tls::TryGet::Taken))
                        });
                    });
                    pending.as_mut().poll(cx)
                })
                .await
            })
        })?;
    let mut store = Store::new(&engine, 101u32);
    let instance = ready(linker.instantiate_async(&mut store, &component))?;
    let func = instance.get_typed_func::<(), ()>(&mut store, "run")?;
    let mut future =
        Box::pin(store.run_concurrent(async |accessor| func.call_concurrent(accessor, ()).await));
    // Initial host polling is on the worker; later polling is on scheduler TLS.
    let payload = if panic_poll == 1 {
        catch_unwind(AssertUnwindSafe(|| {
            let _ = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
        }))
        .expect_err("initial concurrent panic was lost")
    } else {
        for _ in 0..100 {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_tls_empty();
            if observations.polls.lock().unwrap().len() >= 2 {
                break;
            }
        }
        let (payload, catcher) = thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert_tls_empty();
                    let payload = catch_unwind(AssertUnwindSafe(|| {
                        for _ in 0..100 {
                            let _ = future
                                .as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()));
                            assert_tls_empty();
                        }
                    }))
                    .expect_err("migrated scheduler panic was lost");
                    assert_tls_empty();
                    (payload, thread::current().id())
                })
                .join()
                .unwrap()
        });
        assert_eq!(payload.downcast_ref::<Payload>().unwrap().origin, catcher);
        payload
    };
    let polls = observations.polls.lock().unwrap().clone();
    let origin = *polls.last().unwrap();
    if panic_poll > 1 {
        assert_ne!(origin, polls[0]);
    }
    check_panic(|| std::panic::resume_unwind(payload), &observations, origin);
    drop(future);
    assert_tls_empty();
    drop(store);
    assert_eq!(observations.dropped.load(Ordering::SeqCst), 1);
    assert_workers_stopped();
    healthy(&engine)
}

struct HostResource;

fn resource_panic(concurrent: bool, panic_drop: bool) -> Result<()> {
    let engine = engine()?;
    let component = Component::new(
        &engine,
        r#"
        (component
            (import "r" (type $r (sub resource)))
            (core func $drop (canon resource.drop $r))
            (core module $m
                (import "" "drop" (func $drop (param i32)))
                (func $run (export "run") (param i32) (call $drop (local.get 0))))
            (core instance $i (instantiate $m
                (with "" (instance (export "drop" (func $drop))))))
            (func (export "run") (param "r" (own $r))
                (canon lift (core func $i "run"))))
    "#,
    )?;
    let observations = Arc::new(Observations::default());
    let observed = observations.clone();
    let mut linker = Linker::<u32>::new(&engine);
    let panic_poll = if panic_drop { 0 } else { 3 };
    if concurrent {
        linker.root().resource_concurrent(
            "r",
            ResourceType::host::<HostResource>(),
            move |accessor, rep| {
                assert_eq!(rep, 77);
                let mut pending = Box::pin(PendingPanic {
                    observations: observed.clone(),
                    polls: 0,
                    panic_poll,
                    panic_drop,
                });
                Box::pin(async move {
                    core::future::poll_fn(|cx| {
                        accessor.with(|mut access| {
                            assert_eq!(*access.data_mut(), 101);
                            super::tls::try_get(|state| {
                                assert!(matches!(state, super::tls::TryGet::Taken))
                            });
                        });
                        pending.as_mut().poll(cx)
                    })
                    .await
                })
            },
        )?;
    } else {
        linker.root().resource_async(
            "r",
            ResourceType::host::<HostResource>(),
            move |store, rep| {
                assert_eq!(rep, 77);
                assert_eq!(*store.data(), 101);
                Box::new(PendingPanic {
                    observations: observed.clone(),
                    polls: 0,
                    panic_poll,
                    panic_drop,
                })
            },
        )?;
    }
    let mut store = Store::new(&engine, 101u32);
    let instance = ready(linker.instantiate_async(&mut store, &component))?;
    let func = instance.get_typed_func::<(Resource<HostResource>,), ()>(&mut store, "run")?;
    let mut future = Box::pin(func.call_async(&mut store, (Resource::new_own(77),)));
    migrate_pending(future.as_mut());
    let worker = observations.polls.lock().unwrap()[0];
    let payload = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                let payload = catch_unwind(AssertUnwindSafe(|| {
                    if panic_drop {
                        drop(future);
                    } else {
                        let _ = future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()));
                        drop(future);
                    }
                }))
                .expect_err("resource destructor panic was lost");
                assert_tls_empty();
                assert_ne!(thread::current().id(), worker);
                payload
            })
            .join()
            .unwrap()
    });
    check_panic(|| std::panic::resume_unwind(payload), &observations, worker);
    assert_eq!(observations.dropped.load(Ordering::SeqCst), 1);
    assert!(
        observations
            .polls
            .lock()
            .unwrap()
            .iter()
            .all(|id| *id == worker)
    );
    assert_eq!(*observations.drops_on.lock().unwrap(), vec![worker]);
    drop(store);
    assert_workers_stopped();
    healthy(&engine)
}

#[derive(Default)]
struct SiblingObservations {
    panic_value: AtomicUsize,
    polls: Mutex<Vec<(usize, u32, ThreadId)>>,
    next_wait: AtomicUsize,
    dropped: AtomicUsize,
    payload_dropped: AtomicUsize,
}

struct SiblingWait {
    observations: Arc<SiblingObservations>,
    id: usize,
    value: u32,
}

struct SiblingPayload {
    origin: ThreadId,
    observations: Arc<SiblingObservations>,
}

impl Drop for SiblingPayload {
    fn drop(&mut self) {
        self.observations
            .payload_dropped
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl Future for SiblingWait {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let origin = thread::current().id();
        self.observations
            .polls
            .lock()
            .unwrap()
            .push((self.id, self.value, origin));
        if self.observations.panic_value.load(Ordering::SeqCst) == self.value as usize {
            panic_any(SiblingPayload {
                origin,
                observations: self.observations.clone(),
            });
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for SiblingWait {
    fn drop(&mut self) {
        self.observations.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn panic_with_parked_siblings() -> Result<()> {
    assert_tls_empty();
    let mut config = Config::new();
    config.macos_use_mach_ports(false);
    config.wasm_component_model_async(true);
    config.wasm_component_model_more_async_builtins(true);
    config.wasm_component_model_async_stackful(true);
    config.wasm_component_model_threading(true);
    let engine = Engine::new(&config)?;
    let component = Component::new(&engine, THREE_GUEST_COMPONENT)?;
    let observations = Arc::new(SiblingObservations::default());
    let observed = observations.clone();
    let mut linker = Linker::<()>::new(&engine);
    linker
        .root()
        .func_wrap_concurrent("wait", move |_, (value,): (u32,)| {
            let id = observed.next_wait.fetch_add(1, Ordering::SeqCst);
            Box::pin(SiblingWait {
                observations: observed.clone(),
                id,
                value,
            })
        })?;
    linker
        .root()
        .func_wrap("checked", |_, ()| -> Result<()> { Ok(()) })?;
    let mut store = Store::new(&engine, ());
    let workers_before = workers_started();
    let instance = ready(linker.instantiate_async(&mut store, &component))?;
    let func = instance.get_typed_func::<(), ()>(&mut store, "run")?;
    let suspensions_before = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    let mut root =
        Box::pin(store.run_concurrent(async |accessor| func.call_concurrent(accessor, ()).await));
    let first = thread::current().id();
    for _ in 0..100 {
        assert!(
            root.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_tls_empty();
        let polls = observations.polls.lock().unwrap();
        let children = polls
            .iter()
            .filter(|(_, value, _)| *value == 6060)
            .map(|(id, _, _)| *id)
            .collect::<std::collections::HashSet<_>>();
        if children.len() == 2 && polls.iter().any(|(_, value, _)| *value == 5050) {
            break;
        }
    }
    let initial = observations.polls.lock().unwrap().clone();
    assert!(initial.iter().any(|(_, value, _)| *value == 5050));
    assert_eq!(
        initial
            .iter()
            .filter(|(_, value, _)| *value == 6060)
            .map(|(id, _, _)| *id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2,
        "both child guest threads must be waiting"
    );
    let mut first_polls = std::collections::HashMap::new();
    for (id, _, poller) in &initial {
        first_polls.entry(*id).or_insert(*poller);
    }
    assert_eq!(
        first_polls
            .values()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3,
        "all three guests must execute on distinct OS workers"
    );
    assert!(first_polls.values().all(|worker| *worker != first));
    assert!(workers_started() >= workers_before + 3);
    assert!(
        crate::fiber::wasmtime_thread_fiber_tls_suspensions() >= suspensions_before + 3,
        "all three guest stacks must be parked"
    );

    let scheduler = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                let second = thread::current().id();
                for _ in 0..100 {
                    assert!(
                        root.as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    assert_tls_empty();
                    let polls = observations.polls.lock().unwrap();
                    if [5050, 6060].iter().all(|value| {
                        polls
                            .iter()
                            .any(|(_, seen, poller)| seen == value && *poller == second)
                    }) {
                        let children = polls
                            .iter()
                            .filter(|(_, value, poller)| *value == 6060 && *poller == second)
                            .map(|(id, _, _)| *id)
                            .collect::<std::collections::HashSet<_>>()
                            .len();
                        if children == 2 {
                            break;
                        }
                    }
                }
                second
            })
            .join()
            .unwrap()
    });
    assert_ne!(first, scheduler);
    let migrated = observations.polls.lock().unwrap().clone();
    assert!(
        migrated
            .iter()
            .any(|(_, value, poller)| *value == 5050 && *poller == scheduler)
    );
    assert_eq!(
        migrated
            .iter()
            .filter(|(_, value, poller)| *value == 6060 && *poller == scheduler)
            .map(|(id, _, _)| *id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2
    );

    let panic = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                let panic_scheduler = thread::current().id();
                for _ in 0..100 {
                    assert!(
                        root.as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    assert_tls_empty();
                    let polls = observations.polls.lock().unwrap();
                    let children = polls
                        .iter()
                        .filter(|(_, value, poller)| *value == 6060 && *poller == panic_scheduler)
                        .map(|(id, _, _)| *id)
                        .collect::<std::collections::HashSet<_>>()
                        .len();
                    if children == 2
                        && polls
                            .iter()
                            .any(|(_, value, poller)| *value == 5050 && *poller == panic_scheduler)
                    {
                        break;
                    }
                }
                assert_ne!(first, panic_scheduler);
                assert!(
                    observations
                        .polls
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|(_, value, poller)| *value == 5050 && *poller == panic_scheduler),
                    "selected host future was not polled on the panic scheduler"
                );
                observations.panic_value.store(5050, Ordering::SeqCst);
                let payload = catch_unwind(AssertUnwindSafe(|| {
                    for _ in 0..100 {
                        let _ = root.as_mut().poll(&mut Context::from_waker(Waker::noop()));
                        assert_tls_empty();
                    }
                }))
                .expect_err("panic from the selected guest worker was lost");
                assert_tls_empty();
                (payload, panic_scheduler)
            })
            .join()
            .unwrap()
    });
    let (payload, catcher) = panic;
    let payload = payload
        .downcast::<SiblingPayload>()
        .expect("panic payload changed");
    let origin = payload.origin;
    assert_eq!(
        origin, catcher,
        "panic must be caught on its originating poller"
    );
    assert!(
        observations
            .polls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, value, poller)| *value == 5050 && *poller == origin),
        "panic origin was not the thread polling the selected host future"
    );
    assert_eq!(observations.payload_dropped.load(Ordering::SeqCst), 0);
    drop(payload);
    assert_eq!(observations.payload_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.dropped.load(Ordering::SeqCst),
        1,
        "only the panicking host future should be dropped before root cancellation"
    );
    drop(root);
    assert_tls_empty();
    drop(store);
    assert_eq!(observations.dropped.load(Ordering::SeqCst), 3);
    assert_workers_stopped();
    healthy(&engine)
}

macro_rules! panic_test {
    ($name:ident, $run:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::panic_thread_tests::",
                    stringify!($name)
                ),
                $run,
                2,
            )
        }
    };
}

panic_test!(panic_sync_host_worker, || core_panic(0, false));
panic_test!(panic_async_host_first_poll, || core_panic(1, false));
panic_test!(panic_async_host_after_migration, || core_panic(3, false));
panic_test!(panic_async_host_cancel_drop, || core_panic(0, true));
panic_test!(panic_async_resource_poll, || resource_panic(false, false));
panic_test!(panic_async_resource_cancel_drop, || resource_panic(
    false, true
));
panic_test!(panic_concurrent_resource_poll, || resource_panic(
    true, false
));
panic_test!(panic_concurrent_resource_cancel_drop, || resource_panic(
    true, true
));
panic_test!(panic_concurrent_host_first_poll, || concurrent_panic(
    1, false
));
panic_test!(panic_concurrent_host_scheduler, || concurrent_panic(
    4, false
));
panic_test!(panic_worker_with_parked_guest_siblings, || {
    panic_with_parked_siblings()
});
