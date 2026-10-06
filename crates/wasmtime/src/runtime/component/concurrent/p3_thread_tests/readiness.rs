use super::*;
use crate::component::StreamProducer;

const ZERO_LENGTH_READ_COMPONENT: &str = r#"
    (component
        (core module $mem (memory (export "memory") 1))
        (core instance $mem (instantiate $mem))
        (type $s (stream u32))
        (core func $read (canon stream.read $s
            (memory (core memory $mem "memory"))))
        (core func $drop (canon stream.drop-readable $s))
        (core module $m
            (import "" "memory" (memory 1))
            (import "" "read" (func $read (param i32 i32 i32) (result i32)))
            (import "" "drop" (func $drop (param i32)))
            (func (export "run") (param $reader i32)
                (call $read (local.get $reader) (i32.const 0) (i32.const 0))
                i32.const 0
                i32.ne
                if unreachable end
                (call $read (local.get $reader) (i32.const 0) (i32.const 1))
                i32.const 17
                i32.ne
                if unreachable end
                (i32.load (i32.const 0))
                i32.const 42
                i32.ne
                if unreachable end
                (call $drop (local.get $reader))))
        (core instance $i (instantiate $m
            (with "" (instance
                (export "memory" (memory $mem "memory"))
                (export "read" (func $read))
                (export "drop" (func $drop))))))
        (func (export "run") async (param "reader" $s)
            (canon lift (core func $i "run"))))
"#;

#[derive(Default)]
struct ReadinessObservations {
    release_readiness: AtomicBool,
    zero_polls: AtomicUsize,
    item_polls: AtomicUsize,
    completed_readiness: AtomicUsize,
    delivered: AtomicUsize,
    drops: AtomicUsize,
    zero_poll_threads: Mutex<Vec<ThreadId>>,
}

struct ReadinessProducer(Arc<ReadinessObservations>);

impl Drop for ReadinessProducer {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl StreamProducer<u32> for ReadinessProducer {
    type Item = u32;
    type Buffer = Option<u32>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, u32>,
        mut destination: Destination<'a, u32, Option<u32>>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        assert_eq!(*store.data(), 101);
        if finish {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }

        match destination.remaining(store.as_context_mut()) {
            Some(0) => {
                self.0.zero_polls.fetch_add(1, Ordering::SeqCst);
                self.0
                    .zero_poll_threads
                    .lock()
                    .unwrap()
                    .push(thread::current().id());
                if !self.0.release_readiness.load(Ordering::SeqCst) {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    self.0.completed_readiness.fetch_add(1, Ordering::SeqCst);
                    Poll::Ready(Ok(StreamResult::Completed))
                }
            }
            Some(remaining) if remaining > 0 => {
                self.0.item_polls.fetch_add(1, Ordering::SeqCst);
                destination.set_buffer(Some(42));
                self.0.delivered.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Some(_) => unreachable!("guard above handles positive capacity"),
            None => panic!("guest stream reads must report a remaining capacity"),
        }
    }
}

fn ready<T>(mut future: Pin<Box<impl Future<Output = Result<T>>>>) -> Result<T> {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("readiness fixture operation unexpectedly suspended"),
    }
}

fn run_zero_length_readiness() -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, ZERO_LENGTH_READ_COMPONENT)?;
    let mut store = Store::new(&engine, 101u32);
    let instance = ready(Box::pin(
        Linker::new(&engine).instantiate_async(&mut store, &component),
    ))?;
    let observations = Arc::new(ReadinessObservations::default());
    let reader = StreamReader::new(&mut store, ReadinessProducer(observations.clone()))?;
    let run = instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
    let mut call = Box::pin(run.call_async(&mut store, (reader,)));

    let first = thread::current().id();
    for _ in 0..100 {
        assert!(
            call.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_tls_empty();
        if observations.zero_polls.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert!(observations.zero_polls.load(Ordering::SeqCst) > 0);
    assert_eq!(observations.completed_readiness.load(Ordering::SeqCst), 0);
    assert_eq!(observations.item_polls.load(Ordering::SeqCst), 0);
    assert_eq!(observations.delivered.load(Ordering::SeqCst), 0);

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
                        .zero_poll_threads
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
    let zero_poll_threads = observations.zero_poll_threads.lock().unwrap();
    assert!(zero_poll_threads.contains(&first));
    assert!(zero_poll_threads.contains(&second));
    drop(zero_poll_threads);
    assert_eq!(observations.completed_readiness.load(Ordering::SeqCst), 0);
    assert_eq!(observations.item_polls.load(Ordering::SeqCst), 0);
    assert_eq!(observations.delivered.load(Ordering::SeqCst), 0);

    observations.release_readiness.store(true, Ordering::SeqCst);
    for _ in 0..100 {
        let result = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert_tls_empty();
        if let Poll::Ready(result) = result {
            result?;
            break;
        }
    }
    assert!(
        observations.completed_readiness.load(Ordering::SeqCst) > 0,
        "producer did not complete the zero-length readiness probe"
    );
    assert_eq!(observations.item_polls.load(Ordering::SeqCst), 1);
    assert_eq!(observations.delivered.load(Ordering::SeqCst), 1);
    drop(call);
    drop(store);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_workers_stopped();
    assert_tls_empty();
    Ok(())
}

#[test]
fn p3_zero_length_stream_readiness_preserves_item_for_next_read() -> Result<()> {
    isolated_test(
        concat!(
            "runtime::component::concurrent::p3_thread_tests::readiness::",
            "p3_zero_length_stream_readiness_preserves_item_for_next_read"
        ),
        || {
            run_zero_length_readiness()?;
            assert_workers_stopped();
            run_zero_length_readiness()
        },
        2,
    )
}
