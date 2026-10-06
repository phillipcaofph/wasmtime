use super::*;
use crate::component::{Source, StreamConsumer};

const RESOURCE_STREAM_COMPONENT: &str = r#"
    (component
        (import "mark" (func $mark-import))
        (import "r" (type $r (sub resource)))
        (core module $mem
            (memory (export "memory") 1))
        (core instance $mem (instantiate $mem))
        (type $t (stream (own $r)))
        (core func $new (canon stream.new $t))
        (core func $write (canon stream.write $t
            (memory (core memory $mem "memory"))))
        (core func $drop-writable (canon stream.drop-writable $t))
        (core func $resource-drop (canon resource.drop $r))
        (core func $mark-core (canon lower (func $mark-import)))
        (core module $m
            (import "" "memory" (memory 1))
            (import "" "mark" (func $mark))
            (import "" "new" (func $new (result i64)))
            (import "" "write" (func $write (param i32 i32 i32) (result i32)))
            (import "" "drop-writable" (func $drop-writable (param i32)))
            (import "" "resource-drop" (func $resource-drop (param i32)))
            (global $w (mut i32) (i32.const 0))
            (func $start (export "start") (result i32)
                (local $pair i64)
                (local.set $pair (call $new))
                (global.set $w
                    (i32.wrap_i64 (i64.shr_u (local.get $pair) (i64.const 32))))
                (i32.wrap_i64 (local.get $pair)))
            (func $run (export "run") (param $resource i32)
                (call $mark)
                (i32.store (i32.const 0) (local.get $resource))
                (drop (call $write (global.get $w) (i32.const 0) (i32.const 1)))
                (call $drop-writable (global.get $w)))
            (func $drop (export "drop") (param $resource i32)
                (call $resource-drop (local.get $resource))))
        (core instance $i (instantiate $m
            (with "" (instance
                (export "memory" (memory $mem "memory"))
                (export "mark" (func $mark-core))
                (export "new" (func $new))
                (export "write" (func $write))
                (export "drop-writable" (func $drop-writable))
                (export "resource-drop" (func $resource-drop))))))
        (func (export "start") (result $t)
            (canon lift (core func $i "start")))
        (func (export "run") async
            (param "resource" (own $r))
            (canon lift (core func $i "run")))
        (func (export "drop") async
            (param "resource" (own $r))
            (canon lift (core func $i "drop"))))
"#;

fn ready<T>(mut future: Pin<Box<impl Future<Output = Result<T>>>>) -> Result<T> {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("resource fixture operation unexpectedly suspended"),
    }
}

#[derive(Default)]
struct ResourceValueObservations {
    stream: Observations,
    received: Mutex<Option<Resource<HostResource>>>,
    destructor_calls: AtomicUsize,
    worker: Mutex<Option<ThreadId>>,
}

struct ResourceConsumer(Arc<ResourceValueObservations>);

impl StreamConsumer<u32> for ResourceConsumer {
    type Item = Resource<HostResource>;

    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'_, u32>,
        mut source: Source<'_, Self::Item>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        let observations = &self.get_mut().0;
        observations
            .stream
            .polls
            .lock()
            .unwrap()
            .push(thread::current().id());
        if finish {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        if !observations.stream.release.load(Ordering::SeqCst) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let mut value = None;
        source.read(&mut store, &mut value)?;
        let value = value.expect("guest did not write its resource to the stream");
        assert_eq!(value.rep(), 77);
        assert!(
            value.owned(),
            "guest stream transferred a borrowed resource"
        );
        assert!(
            observations
                .received
                .lock()
                .unwrap()
                .replace(value)
                .is_none(),
            "guest stream transferred more than one resource"
        );
        observations.stream.completed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

fn drive_resource_stream<F: Future<Output = Result<()>> + Send>(
    mut call: Pin<&mut F>,
    observations: &ResourceValueObservations,
) -> Result<()> {
    let first = thread::current().id();
    for _ in 0..100 {
        assert!(
            call.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_tls_empty();
        if !observations.stream.polls.lock().unwrap().is_empty() {
            break;
        }
    }

    let second = thread::scope(|scope| {
        scope
            .spawn(|| {
                assert_tls_empty();
                for _ in 0..100 {
                    assert!(
                        call.as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    assert_tls_empty();
                    if observations
                        .stream
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
    {
        let polls = observations.stream.polls.lock().unwrap();
        assert!(polls.contains(&first));
        assert!(polls.contains(&second));
    }
    let worker = observations
        .worker
        .lock()
        .unwrap()
        .expect("guest worker callback did not run");
    assert_ne!(worker, first);
    assert_ne!(worker, second);
    assert_eq!(observations.destructor_calls.load(Ordering::SeqCst), 0);

    observations.stream.release.store(true, Ordering::SeqCst);
    for _ in 0..100 {
        let result = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert_tls_empty();
        if let Poll::Ready(result) = result {
            return result;
        }
    }
    panic!("resource-bearing stream did not complete after release");
}

fn run_resource_value_transfer() -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, RESOURCE_STREAM_COMPONENT)?;
    let mut store = Store::new(&engine, 101u32);
    let observations = Arc::new(ResourceValueObservations::default());
    let observed = observations.clone();
    let mut linker = Linker::<u32>::new(&engine);
    linker
        .root()
        .resource("r", ResourceType::host::<HostResource>(), move |_, rep| {
            assert_eq!(rep, 77);
            observed.destructor_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })?;
    let observed = observations.clone();
    linker.root().func_wrap("mark", move |_, (): ()| {
        let mut worker = observed.worker.lock().unwrap();
        assert!(worker.replace(thread::current().id()).is_none());
        Ok(())
    })?;
    let instance = ready(Box::pin(linker.instantiate_async(&mut store, &component)))?;
    let (reader,) = instance
        .get_typed_func::<(), (StreamReader<Resource<HostResource>>,)>(&mut store, "start")?
        .call(&mut store, ())?;
    reader.pipe(&mut store, ResourceConsumer(observations.clone()))?;

    let run = instance.get_typed_func::<(Resource<HostResource>,), ()>(&mut store, "run")?;
    let mut call = Box::pin(run.call_async(&mut store, (Resource::new_own(77),)));
    drive_resource_stream(call.as_mut(), &observations)?;
    drop(call);
    assert_eq!(
        observations.stream.completed.load(Ordering::SeqCst),
        1,
        "resource-bearing stream did not complete exactly once"
    );

    let resource = observations
        .received
        .lock()
        .unwrap()
        .take()
        .expect("host did not receive the owned resource");
    let drop_resource =
        instance.get_typed_func::<(Resource<HostResource>,), ()>(&mut store, "drop")?;
    ready(Box::pin(drop_resource.call_async(&mut store, (resource,))))?;
    assert_eq!(
        observations.destructor_calls.load(Ordering::SeqCst),
        1,
        "transferred resource was not destroyed exactly once"
    );
    drop(store);
    assert_eq!(
        observations.destructor_calls.load(Ordering::SeqCst),
        1,
        "resource destructor ran again during Store teardown"
    );
    assert_tls_empty();
    Ok(())
}

#[test]
fn p3_stream_transfers_owned_resource_across_worker_and_host() -> Result<()> {
    isolated_test(
        concat!(
            "runtime::component::concurrent::p3_thread_tests::resource_values::",
            "p3_stream_transfers_owned_resource_across_worker_and_host"
        ),
        || {
            run_resource_value_transfer()?;
            assert_workers_stopped();
            run_resource_value_transfer()
        },
        2,
    )
}
