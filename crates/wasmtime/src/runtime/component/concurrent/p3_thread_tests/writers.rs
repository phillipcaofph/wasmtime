use super::*;
use crate::component::{FutureConsumer, Source, StreamConsumer};

#[derive(Default)]
struct Consumption {
    values: Mutex<Vec<u32>>,
    acknowledged: AtomicUsize,
}

struct Consumer {
    producer: Producer,
    consumption: Arc<Consumption>,
    stream: bool,
    backpressure: bool,
    pending_ack: bool,
}

impl Consumer {
    fn consume(
        &mut self,
        cx: &Context<'_>,
        mut store: StoreContextMut<'_, u32>,
        mut source: Source<'_, u32>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        if finish && !self.producer.0.release.load(Ordering::SeqCst) {
            self.producer.0.cancellations.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Ok(StreamResult::Cancelled));
        }
        if self.pending_ack {
            assert_eq!(*store.data(), 101);
            self.producer
                .0
                .polls
                .lock()
                .unwrap()
                .push(thread::current().id());
            if self.producer.0.fail.load(Ordering::SeqCst) {
                return Poll::Ready(Err(crate::format_err!(
                    "intentional P3 consumer acknowledgement error"
                )));
            }
        } else {
            match self.producer.poll(cx, &mut store) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let mut value = None;
            source.read(&mut store, &mut value)?;
            self.consumption
                .values
                .lock()
                .unwrap()
                .push(value.expect("missing guest payload"));
            self.pending_ack = self.backpressure;
        }
        let count = self.consumption.values.lock().unwrap().len();
        if self.pending_ack && self.consumption.acknowledged.load(Ordering::SeqCst) < count {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.pending_ack = false;
        Poll::Ready(Ok(if self.stream && count == 3 {
            StreamResult::Dropped
        } else {
            StreamResult::Completed
        }))
    }
}

impl FutureConsumer<u32> for Consumer {
    type Item = u32;

    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<'_, u32>,
        source: Source<'_, u32>,
        finish: bool,
    ) -> Poll<Result<()>> {
        self.get_mut()
            .consume(cx, store, source, finish)
            .map(|r| r.map(|_| ()))
    }
}

impl StreamConsumer<u32> for Consumer {
    type Item = u32;

    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<'_, u32>,
        source: Source<'_, u32>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        self.get_mut().consume(cx, store, source, finish)
    }
}

fn component(stream: bool, cancel_attempts: u32, async_cancel: bool) -> String {
    let kind = if stream { "stream" } else { "future" };
    let args = if stream { "i32 i32 i32" } else { "i32 i32" };
    let body = if cancel_attempts != 0 {
        let count = if stream { "(i32.const 3)" } else { "" };
        let cancel_locals = if async_cancel {
            "(local $result i32) (local $set i32)"
        } else {
            ""
        };
        let cancel = if async_cancel {
            r#"(local.set $result (call $cancel (global.get $w)))
               (if (i32.eq (local.get $result) (i32.const -1))
                   (then
                       (local.set $set (call $set-new))
                       (call $join (global.get $w) (local.get $set))
                       (drop (call $wait (local.get $set) (i32.const 16)))
                       (call $join (global.get $w) (i32.const 0))
                       (call $set-drop (local.get $set))))
               (if
                   (i32.and
                       (i32.ne (local.get $result) (i32.const -1))
                       (i32.ne (local.get $result) (i32.const 2)))
                   (then unreachable))"#
        } else {
            r#"(call $cancel (global.get $w))
               i32.const 2
               i32.ne
               if unreachable end"#
        };
        format!(
            r#"{cancel_locals}
               (local $attempt i32)
               (loop $retry
                   (call $write (global.get $w) (i32.const 0) {count})
                   i32.const -1
                   i32.ne
                   if unreachable end
                   {cancel}
                   (local.set $attempt (i32.add (local.get $attempt) (i32.const 1)))
                   (br_if $retry (i32.lt_u (local.get $attempt) (i32.const {cancel_attempts}))))"#
        )
    } else if stream {
        r#"(local $count i32) (local $offset i32)
            (loop $write
                (call $write (global.get $w) (local.get $offset)
                    (i32.sub (i32.const 3) (local.get $count)))
                (if (result i32) (i32.eq (local.get $count) (i32.const 2))
                    (then i32.const 17) (else i32.const 16))
                i32.ne
                if unreachable end
                (local.set $count (i32.add (local.get $count) (i32.const 1)))
                (local.set $offset (i32.add (local.get $offset) (i32.const 4)))
                (br_if $write (i32.lt_u (local.get $count) (i32.const 3))))"#
            .to_string()
    } else {
        r#"(call $write (global.get $w) (i32.const 0))
            i32.const 0
            i32.ne
            if unreachable end"#
            .to_string()
    };
    let options = if cancel_attempts != 0 { "async" } else { "" };
    let cancel_options = if async_cancel { "async" } else { "" };
    format!(
        r#"(component
            (core module $mem
                (memory (export "memory") 1)
                (data (i32.const 0) "\2a\00\00\00\2b\00\00\00\2c\00\00\00"))
            (core instance $mem (instantiate $mem))
            (type $t ({kind} u32))
            (core func $new (canon {kind}.new $t))
            (core func $write (canon {kind}.write $t {options} (memory (core memory $mem "memory"))))
            (core func $cancel (canon {kind}.cancel-write $t {cancel_options}))
            (core func $drop (canon {kind}.drop-writable $t))
            (canon waitable.join (core func $join))
            (canon waitable-set.new (core func $set-new))
            (canon waitable-set.wait (memory (core memory $mem "memory")) (core func $wait))
            (canon waitable-set.drop (core func $set-drop))
            (core module $m
                (import "" "new" (func $new (result i64)))
                (import "" "write" (func $write (param {args}) (result i32)))
                (import "" "drop" (func $drop (param i32)))
                (import "" "cancel" (func $cancel (param i32) (result i32)))
                (import "" "join" (func $join (param i32 i32)))
                (import "" "set-new" (func $set-new (result i32)))
                (import "" "wait" (func $wait (param i32 i32) (result i32)))
                (import "" "set-drop" (func $set-drop (param i32)))
                (global $w (mut i32) (i32.const 0))
                (func $start (export "start") (result i32)
                    (local $pair i64)
                    (local.set $pair (call $new))
                    (global.set $w (i32.wrap_i64 (i64.shr_u (local.get $pair) (i64.const 32))))
                    (i32.wrap_i64 (local.get $pair)))
                (func $run (export "run")
                    {body}
                    (call $drop (global.get $w))))
            (core instance $i (instantiate $m
                (with "" (instance
                    (export "set-new" (func $set-new))
                    (export "write" (func $write))
                    (export "cancel" (func $cancel))
                    (export "drop" (func $drop))
                    (export "join" (func $join))
                    (export "new" (func $new))
                    (export "wait" (func $wait))
                    (export "set-drop" (func $set-drop))))))
            (func (export "start") (result $t) (canon lift (core func $i "start")))
            (func (export "run") async (canon lift (core func $i "run"))))"#
    )
}

fn ready<T>(mut future: Pin<Box<impl Future<Output = Result<T>>>>) -> Result<T> {
    let result = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert_tls_empty();
    match result {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("fixture initialization unexpectedly suspended"),
    }
}

fn setup(
    stream: bool,
    cancel_attempts: u32,
    backpressure: bool,
    async_cancel: bool,
) -> Result<(
    Store<u32>,
    crate::component::TypedFunc<(), ()>,
    Arc<Observations>,
    Arc<Consumption>,
)> {
    assert_tls_empty();
    let engine = engine()?;
    let component = Component::new(&engine, component(stream, cancel_attempts, async_cancel))?;
    let mut store = Store::new(&engine, 101u32);
    let linker = Linker::new(&engine);
    let instance = ready(Box::pin(linker.instantiate_async(&mut store, &component)))?;
    let observations = Arc::new(Observations::default());
    let consumption = Arc::new(Consumption::default());
    let consumer = Consumer {
        producer: Producer(observations.clone()),
        consumption: consumption.clone(),
        stream,
        backpressure,
        pending_ack: false,
    };
    if stream {
        let start = instance.get_typed_func::<(), (StreamReader<u32>,)>(&mut store, "start")?;
        let (reader,) = ready(Box::pin(start.call_async(&mut store, ())))?;
        reader.pipe(&mut store, consumer)?;
    } else {
        let start = instance.get_typed_func::<(), (FutureReader<u32>,)>(&mut store, "start")?;
        let (reader,) = ready(Box::pin(start.call_async(&mut store, ())))?;
        reader.pipe(&mut store, consumer)?;
    }
    let func = instance.get_typed_func::<(), ()>(&mut store, "run")?;
    Ok((store, func, observations, consumption))
}

fn run(stream: bool, finish: Finish, backpressure: bool) -> Result<()> {
    let (mut store, func, observations, consumption) = setup(stream, 0, backpressure, false)?;
    let suspensions = crate::fiber::wasmtime_thread_fiber_tls_suspensions();
    let mut call = Box::pin(func.call_async(&mut store, ()));
    if backpressure {
        drive_backpressure(call.as_mut(), &observations, &consumption, finish)?;
    } else {
        drive(call.as_mut(), &observations, finish, true)?;
    }
    drop(call);
    assert!(crate::fiber::wasmtime_thread_fiber_tls_suspensions() > suspensions);
    assert_tls_empty();
    let values = consumption.values.lock().unwrap();
    if matches!(finish, Finish::Complete) {
        assert_eq!(*values, if stream { vec![42, 43, 44] } else { vec![42] });
        assert_eq!(observations.completed.load(Ordering::SeqCst), values.len());
        assert_eq!(
            observations.drops.load(Ordering::SeqCst),
            1,
            "completed consumer retained"
        );
    } else if backpressure {
        assert_eq!(
            *values,
            vec![42],
            "pending item duplicated or later items consumed"
        );
    } else {
        assert!(values.is_empty(), "cancelled/error consumer took a payload");
        assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    }
    drop(store);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_tls_empty();
    Ok(())
}

fn drive_backpressure<F: Future<Output = Result<()>> + Send>(
    mut call: Pin<&mut F>,
    observations: &Observations,
    consumption: &Consumption,
    finish: Finish,
) -> Result<()> {
    // First prove worker-to-scheduler migration while no payload has been taken.
    drive(call.as_mut(), observations, Finish::Cancel, true)?;
    observations.release.store(true, Ordering::SeqCst);
    for expected in 1..=3 {
        for _ in 0..100 {
            assert!(
                call.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_tls_empty();
            if consumption.values.lock().unwrap().len() == expected {
                break;
            }
        }
        assert_eq!(consumption.values.lock().unwrap().len(), expected);
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert_tls_empty();
                    let mut polled = false;
                    for _ in 0..100 {
                        assert!(
                            call.as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()))
                                .is_pending()
                        );
                        assert_tls_empty();
                        assert_eq!(consumption.values.lock().unwrap().len(), expected);
                        if observations
                            .polls
                            .lock()
                            .unwrap()
                            .contains(&thread::current().id())
                        {
                            polled = true;
                            break;
                        }
                    }
                    assert!(polled, "pending acknowledgement did not migrate");
                })
                .join()
                .unwrap();
        });
        assert_eq!(observations.completed.load(Ordering::SeqCst), expected);
        if matches!(finish, Finish::Cancel) {
            return Ok(());
        }
        if matches!(finish, Finish::Error) {
            observations.fail.store(true, Ordering::SeqCst);
            for _ in 0..100 {
                let result = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
                assert_tls_empty();
                if let Poll::Ready(result) = result {
                    let error = result.expect_err("consumer acknowledgement error lost");
                    assert!(
                        format!("{error:#}")
                            .contains("intentional P3 consumer acknowledgement error")
                    );
                    return Ok(());
                }
            }
            panic!("consumer acknowledgement error never delivered");
        }
        consumption.acknowledged.store(expected, Ordering::SeqCst);
    }
    for _ in 0..100 {
        let result = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert_tls_empty();
        if let Poll::Ready(result) = result {
            return result;
        }
    }
    panic!("acknowledged stream did not complete");
}

macro_rules! writer_test {
    ($name:ident, $stream:expr, $finish:ident, $backpressure:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::writers::",
                    stringify!($name)
                ),
                || {
                    run($stream, Finish::$finish, $backpressure)?;
                    assert_workers_stopped();
                    run($stream, Finish::Complete, false)
                },
                2,
            )
        }
    };
}

writer_test!(p3_future_write_completion, false, Complete, false);
writer_test!(p3_future_write_cancellation, false, Cancel, false);
writer_test!(p3_future_write_error, false, Error, false);
writer_test!(p3_stream_write_partial_completion, true, Complete, false);
writer_test!(p3_stream_write_cancellation, true, Cancel, false);
writer_test!(p3_stream_write_error, true, Error, false);
writer_test!(p3_stream_write_backpressure, true, Complete, true);
writer_test!(p3_stream_write_cancel_after_consumption, true, Cancel, true);
writer_test!(p3_stream_write_error_after_consumption, true, Error, true);

fn run_guest_cancel(stream: bool, retry: bool) -> Result<()> {
    let attempts = if retry { 2 } else { 1 };
    let (mut store, func, observations, consumption) = setup(stream, attempts, false, false)?;
    let result = finish_guest_cancellation(Box::pin(func.call_async(&mut store, ())));
    if stream || !retry {
        result?;
        assert_eq!(
            observations.cancellations.load(Ordering::SeqCst),
            attempts as usize
        );
        assert_eq!(
            observations.drops.load(Ordering::SeqCst),
            1,
            "guest close did not retire consumer"
        );
    } else {
        // v48.0.2 marks a future done when a write meets a host consumer,
        // before that consumer completes. Cancellation does not reset it.
        let error = result.expect_err("future write unexpectedly became retryable");
        assert!(format!("{error:#}").contains(
            "cannot write to future after previous write succeeded or readable end dropped"
        ));
        assert_eq!(observations.cancellations.load(Ordering::SeqCst), 1);
    }
    assert!(consumption.values.lock().unwrap().is_empty());
    assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    drop(store);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_tls_empty();
    Ok(())
}

fn run_guest_async_cancel(stream: bool) -> Result<()> {
    let attempts = if stream { 2 } else { 1 };
    let (mut store, func, observations, consumption) = setup(stream, attempts, false, true)?;
    finish_guest_cancellation(Box::pin(func.call_async(&mut store, ())))?;
    assert_eq!(
        observations.cancellations.load(Ordering::SeqCst),
        attempts as usize,
        "async cancel-write did not reach the consumer"
    );
    assert_eq!(observations.completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        observations.drops.load(Ordering::SeqCst),
        1,
        "guest close did not retire consumer"
    );
    assert!(
        consumption.values.lock().unwrap().is_empty(),
        "cancelled consumer took a payload"
    );
    drop(store);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    assert_tls_empty();
    Ok(())
}

macro_rules! guest_cancel_test {
    ($name:ident, $stream:expr, $retry:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::writers::",
                    stringify!($name)
                ),
                || {
                    run_guest_cancel($stream, $retry)?;
                    assert_workers_stopped();
                    run($stream, Finish::Complete, false)
                },
                2,
            )
        }
    };
}

guest_cancel_test!(p3_future_guest_cancel_write_retry_rejected, false, true);
guest_cancel_test!(p3_future_guest_cancel_write_and_close, false, false);
guest_cancel_test!(p3_stream_guest_cancel_write_and_retry, true, true);

macro_rules! guest_async_cancel_test {
    ($name:ident, $stream:expr) => {
        #[test]
        fn $name() -> Result<()> {
            isolated_test(
                concat!(
                    "runtime::component::concurrent::p3_thread_tests::writers::",
                    stringify!($name)
                ),
                || {
                    run_guest_async_cancel($stream)?;
                    assert_workers_stopped();
                    run($stream, Finish::Complete, false)
                },
                2,
            )
        }
    };
}

guest_async_cancel_test!(p3_future_guest_async_cancel_write, false);
guest_async_cancel_test!(p3_stream_guest_async_cancel_write_retry, true);
