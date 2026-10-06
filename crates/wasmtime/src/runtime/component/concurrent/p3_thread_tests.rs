use super::thread_tests::{assert_tls_empty, assert_workers_stopped, isolated_test};
use crate::component::{
    Component, Destination, FutureProducer, FutureReader, Linker, Resource, ResourceType,
    StreamProducer, StreamReader, StreamResult,
};
use crate::prelude::*;
use crate::{AsContextMut, Collector, Config, Engine, Store, StoreContextMut};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread::{self, ThreadId};

mod guest_to_guest;
mod readiness;
mod resource_values;
mod resources_between_guests;
mod writers;

#[derive(Clone, Copy)]
enum Finish {
    Complete,
    Cancel,
    Error,
}

#[derive(Default)]
struct Observations {
    release: AtomicBool,
    fail: AtomicBool,
    drops: AtomicUsize,
    completed: AtomicUsize,
    cancellations: AtomicUsize,
    polls: Mutex<Vec<ThreadId>>,
    worker: Mutex<Option<ThreadId>>,
    collect_parked_roots: AtomicBool,
    parked_root_collections: AtomicUsize,
}

struct Producer(Arc<Observations>);

impl Producer {
    fn poll(&self, cx: &Context<'_>, store: &mut StoreContextMut<'_, u32>) -> Poll<Result<()>> {
        assert_eq!(*store.data(), 101);
        let current = thread::current().id();
        let worker = {
            let mut worker = self.0.worker.lock().unwrap();
            *worker.get_or_insert(current)
        };
        self.0.polls.lock().unwrap().push(current);
        if self.0.collect_parked_roots.load(Ordering::SeqCst)
            && current != worker
            && !self.0.release.load(Ordering::SeqCst)
        {
            assert!(
                store.0.parked_wasm_stack_root_count() > 0,
                "P3 read must retain a GC root on its parked guest stack"
            );
            if let Err(error) = store.gc(None) {
                return Poll::Ready(Err(error));
            }
            self.0
                .parked_root_collections
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.0.release.load(Ordering::SeqCst) {
            if self.0.fail.load(Ordering::SeqCst) {
                Poll::Ready(Err(crate::format_err!("intentional P3 producer error")))
            } else {
                self.0.completed.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl FutureProducer<u32> for Producer {
    type Item = u32;

    fn poll_produce(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'_, u32>,
        finish: bool,
    ) -> Poll<Result<Option<u32>>> {
        if finish && !self.0.release.load(Ordering::SeqCst) {
            self.0.cancellations.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Ok(None));
        }
        self.poll(cx, &mut store).map(|r| r.map(|()| Some(42)))
    }
}

impl StreamProducer<u32> for Producer {
    type Item = u32;
    type Buffer = Option<u32>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, u32>,
        mut destination: Destination<'a, u32, Option<u32>>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        if finish && !self.0.release.load(Ordering::SeqCst) {
            self.0.cancellations.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Ok(StreamResult::Cancelled));
        }
        self.poll(cx, &mut store).map(|r| {
            r.map(|()| {
                destination.set_buffer(Some(42));
                StreamResult::Dropped
            })
        })
    }
}

fn engine() -> Result<Engine> {
    engine_with_collector(None)
}

fn engine_with_collector(collector: Option<Collector>) -> Result<Engine> {
    let mut config = Config::new();
    config.macos_use_mach_ports(false);
    config.wasm_component_model_async(true);
    config.wasm_component_model_more_async_builtins(true);
    config.wasm_component_model_async_stackful(true);
    if let Some(collector) = collector {
        config.collector(collector);
    }
    Engine::new(&config)
}

// Sync reads must park the execution worker, rather than merely returning
// the async ABI's BLOCKED code to a guest that never actually suspends.
fn reader_component(stream: bool, cancel: bool) -> String {
    let kind = if stream { "stream" } else { "future" };
    let count = if stream { "(i32.const 1)" } else { "" };
    let args = if stream { "i32 i32 i32" } else { "i32 i32" };
    let code = if stream { 17 } else { 0 };
    let options = if cancel { "async" } else { "" };
    let read = format!("(call $read (local.get 0) (i32.const 0) {count})");
    let body = if cancel {
        format!(
            r#"(local $attempt i32)
               (loop $retry
                   {read}
                   i32.const -1
                   i32.ne
                   if unreachable end
                   (call $cancel (local.get 0))
                   i32.const 2
                   i32.ne
                   if unreachable end
                   (local.set $attempt (i32.add (local.get $attempt) (i32.const 1)))
                   (br_if $retry (i32.lt_u (local.get $attempt) (i32.const 2))))"#
        )
    } else {
        format!(
            r#"{read}
               i32.const {code}
               i32.ne
               if unreachable end
               (i32.load (i32.const 0))
               i32.const 42
               i32.ne
               if unreachable end"#
        )
    };
    format!(
        r#"(component
            (core module $mem (memory (export "memory") 1))
            (core instance $mem (instantiate $mem))
            (type $t ({kind} u32))
            (core func $read (canon {kind}.read $t {options} (memory (core memory $mem "memory"))))
            (core func $cancel (canon {kind}.cancel-read $t))
            (core func $drop (canon {kind}.drop-readable $t))
            (core module $m
                (import "" "memory" (memory 1))
                (import "" "read" (func $read (param {args}) (result i32)))
                (import "" "drop" (func $drop (param i32)))
                (import "" "cancel" (func $cancel (param i32) (result i32)))
                (func $run (export "run") (param i32)
                    {body}
                    (call $drop (local.get 0))))
            (core instance $i (instantiate $m
                (with "" (instance
                    (export "memory" (memory $mem "memory"))
                    (export "read" (func $read))
                    (export "cancel" (func $cancel))
                    (export "drop" (func $drop))))))
            (func (export "run") async (param "r" $t)
                (canon lift (core func $i "run"))))"#
    )
}

fn reader_component_with_async_cancel(stream: bool) -> String {
    let kind = if stream { "stream" } else { "future" };
    let read_args = if stream { "i32 i32 i32" } else { "i32 i32" };
    let read_count = if stream { "(i32.const 1)" } else { "" };
    format!(
        r#"(component
            (core module $libc (memory (export "memory") 1))
            (core instance $libc (instantiate $libc))
            (type $t ({kind} u32))
            (core module $m
                (import "" "memory" (memory 1))
                (import "" "read" (func $read (param {read_args}) (result i32)))
                (import "" "cancel" (func $cancel (param i32) (result i32)))
                (import "" "drop" (func $drop (param i32)))
                (import "" "join" (func $join (param i32 i32)))
                (import "" "new" (func $new (result i32)))
                (import "" "wait" (func $wait (param i32 i32) (result i32)))
                (import "" "set-drop" (func $set-drop (param i32)))
                (func (export "run") (param $reader i32)
                    (local $attempt i32)
                    (local $result i32)
                    (local $set i32)
                    (loop $retry
                        (call $read (local.get $reader) (i32.const 0) {read_count})
                        i32.const -1
                        i32.ne
                        if unreachable end
                        (local.set $result (call $cancel (local.get $reader)))
                        (if (i32.eq (local.get $result) (i32.const -1))
                            (then
                                (local.set $set (call $new))
                                (call $join (local.get $reader) (local.get $set))
                                (drop (call $wait (local.get $set) (i32.const 16)))
                                (call $join (local.get $reader) (i32.const 0))
                                (call $set-drop (local.get $set))))
                        (if (i32.ne (local.get $result) (i32.const -1))
                            (then
                                (if (i32.ne (local.get $result) (i32.const 2))
                                    (then unreachable))))
                        (local.set $attempt (i32.add (local.get $attempt) (i32.const 1)))
                        (br_if $retry (i32.lt_u (local.get $attempt) (i32.const 2))))
                    (call $drop (local.get $reader))))
            (core func $read (canon {kind}.read $t async (memory (core memory $libc "memory"))))
            (core func $cancel (canon {kind}.cancel-read $t async))
            (core func $drop (canon {kind}.drop-readable $t))
            (canon waitable.join (core func $join))
            (canon waitable-set.new (core func $new))
            (canon waitable-set.wait (memory (core memory $libc "memory")) (core func $wait))
            (canon waitable-set.drop (core func $set-drop))
            (core instance $i (instantiate $m
                (with "" (instance
                    (export "memory" (memory $libc "memory"))
                    (export "read" (func $read))
                    (export "cancel" (func $cancel))
                    (export "drop" (func $drop))
                    (export "join" (func $join))
                    (export "new" (func $new))
                    (export "wait" (func $wait))
                    (export "set-drop" (func $set-drop))))))
            (func (export "run") async (param "r" $t)
                (canon lift (core func $i "run") (memory (core memory $libc "memory")))))"#
    )
}

fn reader_component_with_gc_root(stream: bool) -> String {
    let component = reader_component(stream, false)
        .replace(
            "(core module $m\n                (import \"\" \"memory\" (memory 1))",
            "(core module $m\n                (type $root (struct (field i32)))\n                (import \"\" \"memory\" (memory 1))",
        )
        .replace(
            "(func $run (export \"run\") (param i32)\n                    ",
            "(func $run (export \"run\") (param i32) (local $root (ref $root))\n                    (local.set $root (struct.new $root (i32.const 4242)))\n                    ",
        )
        .replace(
            "(call $drop (local.get 0)))",
            "(call $drop (local.get 0))\n                    (struct.get $root 0 (local.get $root))\n                    i32.const 4242\n                    i32.ne\n                    if unreachable end\n                )",
        );
    assert!(component.contains("(type $root (struct (field i32)))"));
    assert!(component.contains("(struct.get $root 0 (local.get $root))"));
    component
}

fn drive<F: Future<Output = Result<()>> + Send>(
    mut future: Pin<&mut F>,
    observations: &Observations,
    finish: Finish,
    migrate_producer: bool,
) -> Result<()> {
    let first = thread::current().id();
    for _ in 0..100 {
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_tls_empty();
        let polls = observations.polls.lock().unwrap();
        if if migrate_producer {
            polls.contains(&first)
        } else {
            !polls.is_empty()
        } {
            break;
        }
    }
    assert!(
        !observations.polls.lock().unwrap().is_empty(),
        "producer never polled"
    );
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
                    if !migrate_producer
                        || observations
                            .polls
                            .lock()
                            .unwrap()
                            .contains(&thread::current().id())
                    {
                        break;
                    }
                }
                thread::current().id()
            })
            .join()
            .unwrap()
    });
    assert_ne!(first, second);
    assert_eq!(
        observations.drops.load(Ordering::SeqCst),
        0,
        "pending producer destroyed early"
    );
    assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    {
        let polls = observations.polls.lock().unwrap();
        let worker = polls[0];
        assert_ne!(worker, first);
        assert_ne!(worker, second);
        if migrate_producer {
            assert!(
                polls.contains(&first),
                "producer did not reach first scheduler"
            );
            assert!(
                polls.contains(&second),
                "producer did not migrate to second scheduler"
            );
        } else {
            assert!(polls.iter().all(|id| *id == worker));
        }
    }
    if matches!(finish, Finish::Cancel) {
        return Ok(());
    }
    observations
        .fail
        .store(matches!(finish, Finish::Error), Ordering::SeqCst);
    observations.release.store(true, Ordering::SeqCst);
    for _ in 0..100 {
        let result = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert_tls_empty();
        if let Poll::Ready(result) = result {
            match finish {
                Finish::Complete => result?,
                Finish::Error => {
                    let error = result.expect_err("P3 host error was lost");
                    assert!(format!("{error:#}").contains("intentional P3 producer error"));
                }
                Finish::Cancel => unreachable!(),
            }
            return Ok(());
        }
    }
    panic!("released P3 operation never completed");
}

fn run_reader(stream: bool, finish: Finish) -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, reader_component(stream, false))?;
    let mut store = Store::new(&engine, 101u32);
    let linker = Linker::new(&engine);
    let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
    let instance = match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r?,
        Poll::Pending => panic!("unexpected instantiation suspension"),
    };
    drop(init);
    let observations = Arc::new(Observations::default());
    let suspensions = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    if stream {
        let func = instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
        let reader = StreamReader::new(&mut store, Producer(observations.clone()))?;
        let mut call = Box::pin(func.call_async(&mut store, (reader,)));
        drive(call.as_mut(), &observations, finish, true)?;
    } else {
        let func = instance.get_typed_func::<(FutureReader<u32>,), ()>(&mut store, "run")?;
        let reader = FutureReader::new(&mut store, Producer(observations.clone()))?;
        let mut call = Box::pin(func.call_async(&mut store, (reader,)));
        drive(call.as_mut(), &observations, finish, true)?;
    }
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() > suspensions);
    assert_tls_empty();
    if matches!(finish, Finish::Complete) {
        assert_eq!(
            observations.drops.load(Ordering::SeqCst),
            1,
            "completed producer was not retired promptly"
        );
    }
    drop(store);
    assert_tls_empty();
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.completed.load(Ordering::SeqCst),
        usize::from(matches!(finish, Finish::Complete))
    );
    Ok(())
}

fn run_parked_root_reader(stream: bool, collector: Collector) -> Result<()> {
    assert_tls_empty();
    let engine = engine_with_collector(Some(collector))?;
    let component = Component::new(&engine, reader_component_with_gc_root(stream))?;
    let mut store = Store::new(&engine, 101u32);
    let linker = Linker::new(&engine);
    let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
    let instance = match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result?,
        Poll::Pending => panic!("unexpected instantiation suspension"),
    };
    drop(init);

    let observations = Arc::new(Observations::default());
    observations
        .collect_parked_roots
        .store(true, Ordering::SeqCst);
    let suspensions = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    if stream {
        let func = instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
        let reader = StreamReader::new(&mut store, Producer(observations.clone()))?;
        drive(
            Box::pin(func.call_async(&mut store, (reader,))).as_mut(),
            &observations,
            Finish::Complete,
            true,
        )?;
    } else {
        let func = instance.get_typed_func::<(FutureReader<u32>,), ()>(&mut store, "run")?;
        let reader = FutureReader::new(&mut store, Producer(observations.clone()))?;
        drive(
            Box::pin(func.call_async(&mut store, (reader,))).as_mut(),
            &observations,
            Finish::Complete,
            true,
        )?;
    }
    assert!(
        observations.parked_root_collections.load(Ordering::SeqCst) > 0,
        "no GC ran while a guest stack was parked"
    );
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() > suspensions);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_eq!(observations.completed.load(Ordering::SeqCst), 1);
    drop(store);
    assert_workers_stopped();
    assert_tls_empty();
    Ok(())
}

macro_rules! parked_root_reader_test {
    ($name:ident, $stream:expr, $collector:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::",
                    stringify!($name)
                ),
                || {
                    run_parked_root_reader($stream, $collector)?;
                    assert_workers_stopped();
                    run_parked_root_reader($stream, $collector)
                },
                2,
            )
        }
    };
}

parked_root_reader_test!(
    p3_future_read_gc_while_guest_stack_parked_drc,
    false,
    Collector::DeferredReferenceCounting
);
parked_root_reader_test!(
    p3_stream_read_gc_while_guest_stack_parked_drc,
    true,
    Collector::DeferredReferenceCounting
);
#[cfg(feature = "gc-copying")]
parked_root_reader_test!(
    p3_future_read_gc_while_guest_stack_parked_copying,
    false,
    Collector::Copying
);
#[cfg(feature = "gc-copying")]
parked_root_reader_test!(
    p3_stream_read_gc_while_guest_stack_parked_copying,
    true,
    Collector::Copying
);

macro_rules! reader_test {
    ($name:ident, $stream:expr, $finish:ident) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::",
                    stringify!($name)
                ),
                || {
                    run_reader($stream, Finish::$finish)?;
                    assert_workers_stopped();
                    run_reader($stream, Finish::Complete)
                },
                2,
            )
        }
    };
}

reader_test!(p3_future_pending_completion, false, Complete);
reader_test!(p3_future_pending_cancellation, false, Cancel);
reader_test!(p3_future_pending_error, false, Error);
reader_test!(p3_stream_pending_completion, true, Complete);
reader_test!(p3_stream_pending_cancellation, true, Cancel);
reader_test!(p3_stream_pending_error, true, Error);

fn run_guest_cancellation(stream: bool) -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, reader_component(stream, true))?;
    let mut store = Store::new(&engine, 101u32);
    let linker = Linker::new(&engine);
    let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
    let instance = match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r?,
        Poll::Pending => panic!("unexpected instantiation suspension"),
    };
    drop(init);
    let observations = Arc::new(Observations::default());
    if stream {
        let func = instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
        let reader = StreamReader::new(&mut store, Producer(observations.clone()))?;
        finish_guest_cancellation(Box::pin(func.call_async(&mut store, (reader,))))?;
    } else {
        let func = instance.get_typed_func::<(FutureReader<u32>,), ()>(&mut store, "run")?;
        let reader = FutureReader::new(&mut store, Producer(observations.clone()))?;
        finish_guest_cancellation(Box::pin(func.call_async(&mut store, (reader,))))?;
    }
    assert_eq!(observations.cancellations.load(Ordering::SeqCst), 2);
    assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        observations.drops.load(Ordering::SeqCst),
        1,
        "guest close did not retire producer"
    );
    drop(store);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_tls_empty();
    Ok(())
}

fn run_guest_async_cancellation(stream: bool) -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, reader_component_with_async_cancel(stream))?;
    let mut store = Store::new(&engine, 101u32);
    let linker = Linker::new(&engine);
    let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
    let instance = match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result?,
        Poll::Pending => panic!("unexpected instantiation suspension"),
    };
    drop(init);
    let observations = Arc::new(Observations::default());
    if stream {
        let func = instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
        let reader = StreamReader::new(&mut store, Producer(observations.clone()))?;
        finish_guest_cancellation(Box::pin(func.call_async(&mut store, (reader,))))?;
    } else {
        let func = instance.get_typed_func::<(FutureReader<u32>,), ()>(&mut store, "run")?;
        let reader = FutureReader::new(&mut store, Producer(observations.clone()))?;
        finish_guest_cancellation(Box::pin(func.call_async(&mut store, (reader,))))?;
    }
    assert_eq!(
        observations.cancellations.load(Ordering::SeqCst),
        2,
        "async cancel-read did not reach producer twice"
    );
    assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    drop(store);
    assert_workers_stopped();
    assert_tls_empty();
    Ok(())
}

fn finish_guest_cancellation(mut future: Pin<Box<impl Future<Output = Result<()>>>>) -> Result<()> {
    for _ in 0..100 {
        let result = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert_tls_empty();
        if let Poll::Ready(result) = result {
            return result;
        }
    }
    panic!("guest cancel-and-retry never completed");
}

macro_rules! guest_cancel_test {
    ($name:ident, $stream:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::",
                    stringify!($name)
                ),
                || {
                    run_guest_cancellation($stream)?;
                    assert_workers_stopped();
                    run_reader($stream, Finish::Complete)
                },
                2,
            )
        }
    };
}

guest_cancel_test!(p3_future_guest_cancel_and_retry, false);
guest_cancel_test!(p3_stream_guest_cancel_and_retry, true);

macro_rules! guest_async_cancel_test {
    ($name:ident, $stream:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::",
                    stringify!($name)
                ),
                || {
                    run_guest_async_cancellation($stream)?;
                    assert_workers_stopped();
                    run_reader($stream, Finish::Complete)
                },
                2,
            )
        }
    };
}

guest_async_cancel_test!(p3_future_guest_async_cancel_and_retry, false);
guest_async_cancel_test!(p3_stream_guest_async_cancel_and_retry, true);

const RESOURCE_COMPONENT: &str = r#"
    (component
        (import "r" (type $r (sub resource)))
        (core func $drop (canon resource.drop $r))
        (core module $m
            (import "" "drop" (func $drop (param i32)))
            (func $run (export "run") (param i32)
                (call $drop (local.get 0))))
        (core instance $i (instantiate $m
            (with "" (instance (export "drop" (func $drop))))))
        (func (export "run") (param "r" (own $r))
            (canon lift (core func $i "run"))))
"#;

struct HostResource;

fn run_resource(concurrent: bool, finish: Finish) -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, RESOURCE_COMPONENT)?;
    let mut store = Store::new(&engine, 101u32);
    let observations = Arc::new(Observations::default());
    let observed = observations.clone();
    let mut linker = Linker::<u32>::new(&engine);
    if concurrent {
        linker.root().resource_concurrent(
            "r",
            ResourceType::host::<HostResource>(),
            move |accessor, rep| {
                assert_eq!(rep, 77);
                let producer = Producer(observed.clone());
                Box::pin(async move {
                    core::future::poll_fn(|cx| {
                        accessor.with(|mut access| {
                            super::tls::try_get(|state| {
                                assert!(matches!(state, super::tls::TryGet::Taken));
                            });
                            producer.poll(cx, &mut access.as_context_mut())
                        })
                    })
                    .await
                })
            },
        )?;
    } else {
        linker.root().resource_async(
            "r",
            ResourceType::host::<HostResource>(),
            move |mut store, rep| {
                assert_eq!(rep, 77);
                let producer = Producer(observed.clone());
                Box::new(
                    async move { core::future::poll_fn(|cx| producer.poll(cx, &mut store)).await },
                )
            },
        )?;
    }
    let mut init = Box::pin(linker.instantiate_async(&mut store, &component));
    let instance = match init.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r?,
        Poll::Pending => panic!("unexpected instantiation suspension"),
    };
    drop(init);
    let func = instance.get_typed_func::<(Resource<HostResource>,), ()>(&mut store, "run")?;
    let suspensions = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    let mut call = Box::pin(func.call_async(&mut store, (Resource::new_own(77),)));
    drive(call.as_mut(), &observations, finish, false)?;
    drop(call);
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() > suspensions);
    assert_eq!(
        observations.drops.load(Ordering::SeqCst),
        1,
        "destructor future leaked"
    );
    let polls = observations.polls.lock().unwrap();
    let worker = polls[0];
    assert_ne!(worker, thread::current().id());
    assert!(
        polls.iter().all(|id| *id == worker),
        "destructor moved off its execution worker"
    );
    assert_eq!(
        observations.completed.load(Ordering::SeqCst),
        usize::from(matches!(finish, Finish::Complete))
    );
    drop(store);
    assert_eq!(
        observations.drops.load(Ordering::SeqCst),
        1,
        "destructor ran twice"
    );
    assert_tls_empty();
    Ok(())
}

macro_rules! resource_test {
    ($name:ident, $concurrent:expr, $finish:ident) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::",
                    stringify!($name)
                ),
                || {
                    run_resource($concurrent, Finish::$finish)?;
                    assert_workers_stopped();
                    run_resource($concurrent, Finish::Complete)
                },
                2,
            )
        }
    };
}

resource_test!(p3_async_resource_pending_completion, false, Complete);
resource_test!(p3_async_resource_pending_cancellation, false, Cancel);
resource_test!(p3_async_resource_pending_error, false, Error);
resource_test!(p3_concurrent_resource_pending_completion, true, Complete);
resource_test!(p3_concurrent_resource_pending_cancellation, true, Cancel);
resource_test!(p3_concurrent_resource_pending_error, true, Error);
