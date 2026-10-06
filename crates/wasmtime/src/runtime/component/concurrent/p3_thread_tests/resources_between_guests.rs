use super::*;

const RESOURCE_STREAM_WRITER: &str = r#"
    (component
        (import "r" (type $r (sub resource)))
        (import "writer-mark" (func $mark-import))
        (core func $mark-core (canon lower (func $mark-import)))
        (core module $mem (memory (export "memory") 1))
        (core instance $mem (instantiate $mem))
        (type $s (stream (own $r)))
        (core func $new (canon stream.new $s))
        (core func $write (canon stream.write $s
            (memory (core memory $mem "memory"))))
        (core func $drop-writable (canon stream.drop-writable $s))
        (core module $m
            (import "" "memory" (memory 1))
            (import "" "mark" (func $mark))
            (import "" "new" (func $new (result i64)))
            (import "" "write" (func $write (param i32 i32 i32) (result i32)))
            (import "" "drop-writable" (func $drop-writable (param i32)))
            (global $w (mut i32) (i32.const 0))
            (func (export "start") (result i32)
                (local $pair i64)
                (local.set $pair (call $new))
                (global.set $w
                    (i32.wrap_i64 (i64.shr_u (local.get $pair) (i64.const 32))))
                (i32.wrap_i64 (local.get $pair)))
            (func (export "run") (param $resource i32)
                (call $mark)
                (i32.store (i32.const 0) (local.get $resource))
                (drop (call $write (global.get $w) (i32.const 0) (i32.const 1)))
                (call $drop-writable (global.get $w))))
        (core instance $i (instantiate $m
            (with "" (instance
                (export "memory" (memory $mem "memory"))
                (export "mark" (func $mark-core))
                (export "new" (func $new))
                (export "write" (func $write))
                (export "drop-writable" (func $drop-writable))))))
        (func (export "start") (result $s)
            (canon lift (core func $i "start")))
        (func (export "run") async (param "resource" (own $r))
            (canon lift (core func $i "run"))))
"#;

const RESOURCE_STREAM_READER: &str = r#"
    (component
        (import "r" (type $r (sub resource)))
        (import "reader-mark" (func $mark-import))
        (core func $mark-core (canon lower (func $mark-import)))
        (core module $mem (memory (export "memory") 1))
        (core instance $mem (instantiate $mem))
        (type $s (stream (own $r)))
        (core func $read (canon stream.read $s
            (memory (core memory $mem "memory"))))
        (core func $drop-readable (canon stream.drop-readable $s))
        (core func $resource-drop (canon resource.drop $r))
        (core module $m
            (import "" "memory" (memory 1))
            (import "" "mark" (func $mark))
            (import "" "read" (func $read (param i32 i32 i32) (result i32)))
            (import "" "drop-readable" (func $drop-readable (param i32)))
            (import "" "resource-drop" (func $resource-drop (param i32)))
            (func (export "run") (param $reader i32)
                (call $mark)
                (call $read (local.get $reader) (i32.const 0) (i32.const 1))
                i32.const 16
                i32.ne
                if unreachable end
                (i32.load (i32.const 0))
                (call $resource-drop)
                (call $drop-readable (local.get $reader))))
        (core instance $i (instantiate $m
            (with "" (instance
                (export "memory" (memory $mem "memory"))
                (export "mark" (func $mark-core))
                (export "read" (func $read))
                (export "drop-readable" (func $drop-readable))
                (export "resource-drop" (func $resource-drop))))))
        (func (export "run") async (param "reader" $s)
            (canon lift (core func $i "run")
                (memory (core memory $mem "memory")))))
"#;

struct GuestTransferResource;

fn ready<T>(mut future: Pin<Box<impl Future<Output = Result<T>>>>) -> Result<T> {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("resource fixture operation unexpectedly suspended"),
    }
}

fn run_guest_to_guest_resource_transfer() -> Result<()> {
    assert_tls_empty();
    let engine = engine()?;
    let writer_component = Component::new(&engine, RESOURCE_STREAM_WRITER)?;
    let reader_component = Component::new(&engine, RESOURCE_STREAM_READER)?;
    let mut store = Store::new(&engine, 101u32);

    let drops = Arc::new(AtomicUsize::new(0));
    let dropped = drops.clone();
    let writer_worker = Arc::new(Mutex::new(None));
    let writer_worker_observed = writer_worker.clone();
    let reader_worker = Arc::new(Mutex::new(None));
    let reader_worker_observed = reader_worker.clone();
    let mut linker = Linker::<u32>::new(&engine);
    linker.root().resource(
        "r",
        ResourceType::host::<GuestTransferResource>(),
        move |_, rep| {
            assert_eq!(rep, 77);
            dropped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )?;
    linker.root().func_wrap("writer-mark", move |_, (): ()| {
        assert!(
            writer_worker_observed
                .lock()
                .unwrap()
                .replace(thread::current().id())
                .is_none(),
            "writer guest marker ran more than once"
        );
        Ok(())
    })?;
    linker.root().func_wrap("reader-mark", move |_, (): ()| {
        assert!(
            reader_worker_observed
                .lock()
                .unwrap()
                .replace(thread::current().id())
                .is_none(),
            "reader guest marker ran more than once"
        );
        Ok(())
    })?;

    let writer = ready(Box::pin(
        linker.instantiate_async(&mut store, &writer_component),
    ))?;
    let reader = ready(Box::pin(
        linker.instantiate_async(&mut store, &reader_component),
    ))?;
    let (stream,) = writer
        .get_typed_func::<(), (StreamReader<Resource<GuestTransferResource>>,)>(
            &mut store, "start",
        )?
        .call(&mut store, ())?;
    let write =
        writer.get_typed_func::<(Resource<GuestTransferResource>,), ()>(&mut store, "run")?;
    let read = reader.get_typed_func::<(StreamReader<Resource<GuestTransferResource>>,), ()>(
        &mut store, "run",
    )?;

    ready(Box::pin(store.run_concurrent(async |accessor| {
        let mut write_call = Box::pin(write.call_concurrent(&accessor, (Resource::new_own(77),)));
        let mut read_call = Box::pin(read.call_concurrent(&accessor, (stream,)));
        let mut write_result = None;
        let mut read_result = None;
        core::future::poll_fn(|cx| {
            if write_result.is_none() {
                if let Poll::Ready(result) = write_call.as_mut().poll(cx) {
                    write_result = Some(result);
                }
            }
            if read_result.is_none() {
                if let Poll::Ready(result) = read_call.as_mut().poll(cx) {
                    read_result = Some(result);
                }
            }
            if write_result.is_some() && read_result.is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        write_result.expect("writer call did not complete")?;
        read_result.expect("reader call did not complete")?;
        Ok::<(), crate::Error>(())
    })))??;

    let writer_worker = writer_worker
        .lock()
        .unwrap()
        .expect("writer guest did not execute");
    let reader_worker = reader_worker
        .lock()
        .unwrap()
        .expect("reader guest did not execute");
    assert_ne!(writer_worker, reader_worker);
    assert_ne!(writer_worker, thread::current().id());
    assert_ne!(reader_worker, thread::current().id());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(store);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_workers_stopped();
    assert_tls_empty();
    Ok(())
}

#[test]
fn p3_stream_transfers_owned_resource_between_guest_instances() -> Result<()> {
    isolated_test(
        concat!(
            "runtime::component::concurrent::p3_thread_tests::resources_between_guests::",
            "p3_stream_transfers_owned_resource_between_guest_instances"
        ),
        || {
            run_guest_to_guest_resource_transfer()?;
            assert_workers_stopped();
            run_guest_to_guest_resource_transfer()
        },
        2,
    )
}
