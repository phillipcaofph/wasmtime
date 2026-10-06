use super::*;

const FORWARD_STREAM_COMPONENT: &str = r#"
    (component
        (import "mark" (func $mark-import))
        (type $s (stream u32))
        (type $forward-type (func async (param "reader" $s)))
        (import "forward" (func $forward (type $forward-type)))
        (core func $forward-core (canon lower (func $forward)))
        (core func $mark-core (canon lower (func $mark-import)))
        (core module $m
            (import "" "forward" (func $forward (param i32)))
            (import "" "mark" (func $mark))
            (func (export "run") (param i32)
                (call $mark)
                (call $forward (local.get 0))))
        (core instance $i (instantiate $m
            (with "" (instance
                (export "forward" (func $forward-core))
                (export "mark" (func $mark-core))))))
        (func (export "run") async (param "reader" $s)
            (canon lift (core func $i "run"))))
"#;

const GUEST_STREAM_READER_COMPONENT: &str = r#"
    (component
        (import "mark" (func $mark-import))
        (core func $mark-core (canon lower (func $mark-import)))
        (core module $mem (memory (export "memory") 1))
        (core instance $mem (instantiate $mem))
        (type $s (stream u32))
        (core func $read (canon stream.read $s
            (memory (core memory $mem "memory"))))
        (core func $drop (canon stream.drop-readable $s))
        (core module $m
            (import "" "mark" (func $mark))
            (import "" "memory" (memory 1))
            (import "" "read" (func $read (param i32 i32 i32) (result i32)))
            (import "" "drop" (func $drop (param i32)))
            (func (export "run") (param $reader i32)
                (call $mark)
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
                (export "mark" (func $mark-core))
                (export "memory" (memory $mem "memory"))
                (export "read" (func $read))
                (export "drop" (func $drop))))))
        (func (export "run") async (param "reader" $s)
            (canon lift (core func $i "run")
                (memory (core memory $mem "memory")))))
"#;

fn ready<T>(mut future: Pin<Box<impl Future<Output = Result<T>>>>) -> Result<T> {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("guest component operation unexpectedly suspended"),
    }
}

fn run_guest_to_guest_stream_transfer() -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let forwarding_component = Component::new(&engine, FORWARD_STREAM_COMPONENT)?;
    let reader_component = Component::new(&engine, GUEST_STREAM_READER_COMPONENT)?;
    let mut store = Store::new(&engine, 101u32);

    let reader_worker = Arc::new(Mutex::new(None));
    let observed_reader_worker = reader_worker.clone();
    let mut reader_linker = Linker::<u32>::new(&engine);
    reader_linker.root().func_wrap("mark", move |_, (): ()| {
        assert!(
            observed_reader_worker
                .lock()
                .unwrap()
                .replace(thread::current().id())
                .is_none(),
            "reader guest marker ran more than once"
        );
        Ok(())
    })?;
    let reader_instance = ready(Box::pin(
        reader_linker.instantiate_async(&mut store, &reader_component),
    ))?;
    let reader_instance = Arc::new(Mutex::new(Some(reader_instance)));
    let target_instance = reader_instance.clone();

    let forwarding_worker = Arc::new(Mutex::new(None));
    let observed_forwarding_worker = forwarding_worker.clone();
    let mut linker = Linker::<u32>::new(&engine);
    linker.root().func_wrap("mark", move |_, (): ()| {
        assert!(
            observed_forwarding_worker
                .lock()
                .unwrap()
                .replace(thread::current().id())
                .is_none(),
            "forwarding guest marker ran more than once"
        );
        Ok(())
    })?;
    linker.root().func_wrap_concurrent(
        "forward",
        move |accessor, (stream,): (StreamReader<u32>,)| {
            let target = accessor.with(|mut store| {
                target_instance
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("reader component instance missing")
                    .get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")
            });
            Box::pin(async move {
                let target = target?;
                target.call_concurrent(accessor, (stream,)).await
            })
        },
    )?;
    let forwarding_instance = ready(Box::pin(
        linker.instantiate_async(&mut store, &forwarding_component),
    ))?;

    let observations = Arc::new(Observations::default());
    let stream = StreamReader::new(&mut store, Producer(observations.clone()))?;
    let run = forwarding_instance.get_typed_func::<(StreamReader<u32>,), ()>(&mut store, "run")?;
    let mut call = Box::pin(run.call_async(&mut store, (stream,)));
    drive(call.as_mut(), &observations, Finish::Complete, true)?;

    let forwarding_worker = forwarding_worker
        .lock()
        .unwrap()
        .expect("forwarding guest did not execute");
    let reader_worker = reader_worker
        .lock()
        .unwrap()
        .expect("reader guest did not execute");
    assert_ne!(forwarding_worker, reader_worker);
    assert_ne!(forwarding_worker, thread::current().id());
    assert_ne!(reader_worker, thread::current().id());
    assert_eq!(observations.completed.load(Ordering::SeqCst), 1);
    assert_eq!(observations.drops.load(Ordering::SeqCst), 1);
    drop(call);
    drop(store);
    assert_workers_stopped();
    assert_tls_empty();
    Ok(())
}

#[test]
fn p3_stream_reader_forwarded_between_guest_instances() -> Result<()> {
    isolated_test(
        concat!(
            "runtime::component::concurrent::p3_thread_tests::guest_to_guest::",
            "p3_stream_reader_forwarded_between_guest_instances"
        ),
        || {
            run_guest_to_guest_stream_transfer()?;
            assert_workers_stopped();
            run_guest_to_guest_stream_transfer()
        },
        2,
    )
}
