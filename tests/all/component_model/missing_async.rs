#![cfg(not(miri))] // not testing unsafe code

use wasmtime::component::{Component, Func, Linker, ResourceAny};
use wasmtime::{Config, Engine, Result, Store};

fn async_store() -> Store<()> {
    let engine = Engine::default();
    let mut store = Store::new(&engine, ());
    wasmtime::Func::wrap_async(&mut store, |_, ()| Box::new(async {}));
    return store;
}

async fn func_in_store(store: &mut Store<()>) -> Result<Func> {
    let component = Component::new(
        store.engine(),
        r#"
            (component
                (core module $a
                    (func (export "hi") )
                )
                (core instance $i (instantiate $a))
                (func (export "hi") (canon lift (core func $i "hi")))
            )
        "#,
    )?;
    let instance = Linker::new(store.engine())
        .instantiate_async(&mut *store, &component)
        .await?;
    let func = instance.get_func(&mut *store, "hi").unwrap();
    Ok(func)
}

fn assert_requires_async<T>(store: &mut Store<T>) {
    let module = wasmtime::Module::new(store.engine(), "(module)").unwrap();
    assert!(wasmtime::Instance::new(&mut *store, &module, &[]).is_err());
}

#[tokio::test]
async fn async_disallows_func_call() -> Result<()> {
    let mut store = async_store();
    let func = func_in_store(&mut store).await?;
    assert!(func.call(&mut store, &[], &mut []).is_err());
    func.call_async(&mut store, &[], &mut []).await?;
    Ok(())
}

#[tokio::test]
async fn async_disallows_typed_func_call() -> Result<()> {
    let mut store = async_store();
    let func = func_in_store(&mut store).await?;
    let func = func.typed::<(), ()>(&store)?;
    assert!(func.call(&mut store, ()).is_err());
    func.call_async(&mut store, ()).await?;
    Ok(())
}

#[tokio::test]
async fn async_disallows_instantiate() -> Result<()> {
    let mut store = async_store();
    let component = Component::new(store.engine(), "(component)")?;
    let linker = Linker::new(store.engine());
    assert!(linker.instantiate(&mut store, &component).is_err());
    linker.instantiate_async(&mut store, &component).await?;
    Ok(())
}

#[tokio::test]
async fn require_async_after_linker_with_func_wrap_async() -> Result<()> {
    let engine = Engine::default();
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_wrap_async("hi", |_, ()| Box::new(async { Ok(()) }))?;
    let module = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func))
                (core func (canon lower (func 0)))
            )
        "#,
    )?;
    linker.instantiate_async(&mut store, &module).await?;
    assert_requires_async(&mut store);
    Ok(())
}

#[tokio::test]
async fn require_async_after_linker_with_func_wrap_concurrent() -> Result<()> {
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_wrap_concurrent("hi", |_, ()| Box::pin(async { Ok(()) }))?;
    let module = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func async))
                (core func (canon lower (func 0)))
            )
        "#,
    )?;
    linker.instantiate_async(&mut store, &module).await?;
    assert_requires_async(&mut store);
    Ok(())
}

#[tokio::test]
async fn require_async_after_linker_with_func_new_async() -> Result<()> {
    let engine = Engine::default();
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_new_async("hi", |_, _, _, _| Box::new(async { Ok(()) }))?;
    let module = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func))
                (core func (canon lower (func 0)))
            )
        "#,
    )?;
    linker.instantiate_async(&mut store, &module).await?;
    assert_requires_async(&mut store);
    Ok(())
}

#[tokio::test]
async fn require_async_after_linker_with_func_new_concurrent() -> Result<()> {
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_new_concurrent("hi", |_, _, _, _| Box::pin(async { Ok(()) }))?;
    let module = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func async))
                (core func (canon lower (func 0)))
            )
        "#,
    )?;
    linker.instantiate_async(&mut store, &module).await?;
    assert_requires_async(&mut store);
    Ok(())
}

#[tokio::test]
async fn func_new_async_cannot_satisfy_async_import() -> Result<()> {
    // `func_new_async` (like `func_wrap_async`) intentionally implements a
    // *sync*-WIT-typed function via blocking host code - it can never
    // satisfy an `async func` import, only `func_new_concurrent` can. The
    // error message should say so specifically, not just "type mismatch
    // with async" with no indication of what to do about it.
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_new_async("hi", |_, _, _, _| Box::new(async { Ok(()) }))?;
    let component = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func async))
                (core func (canon lower (func 0) async))
            )
        "#,
    )?;
    let err = linker
        .instantiate_async(&mut store, &component)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("declared `async func` in WIT"), "{msg}");
    assert!(msg.contains("func_new_concurrent"), "{msg}");
    Ok(())
}

#[tokio::test]
async fn func_new_concurrent_cannot_satisfy_sync_import() -> Result<()> {
    // The reverse mismatch: `func_new_concurrent` is only for `async
    // func`-typed imports, and shouldn't silently satisfy a plain one either.
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .func_new_concurrent("hi", |_, _, _, _| Box::pin(async { Ok(()) }))?;
    let component = Component::new(
        store.engine(),
        r#"
            (component
                (import "hi" (func))
                (core func (canon lower (func 0)))
            )
        "#,
    )?;
    let err = linker
        .instantiate_async(&mut store, &component)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("plain (non-`async`) function"), "{msg}");
    assert!(msg.contains("func_new"), "{msg}");
    Ok(())
}

#[tokio::test]
async fn async_disallows_resource_any_drop() -> Result<()> {
    let mut store = async_store();
    let component = Component::new(
        store.engine(),
        r#"
            (component
                (type $t' (resource (rep i32)))
                (export $t "t" (type $t'))

                (core func $new (canon resource.new $t))
                (func (export "mk") (param "r" u32) (result (own $t))
                    (canon lift (core func $new))
                )
            )
        "#,
    )?;
    let instance = Linker::new(store.engine())
        .instantiate_async(&mut store, &component)
        .await?;
    let func = instance.get_typed_func::<(u32,), (ResourceAny,)>(&mut store, "mk")?;
    let (resource,) = func.call_async(&mut store, (42,)).await?;

    assert!(resource.resource_drop(&mut store).is_err());
    resource.resource_drop_async(&mut store).await?;

    Ok(())
}

#[tokio::test]
async fn async_store_allows_borrow_drop() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wasmtime::component::{Resource, ResourceType, Val};

    let mut store = async_store();

    // A synthetic borrow (no host table entry) can be dropped synchronously.
    let synthetic = Resource::<u32>::new_borrow(1).try_into_resource_any(&mut store)?;
    synthetic.resource_drop(&mut store)?;

    // An owned resource still requires the async drop.
    let owned = Resource::<u32>::new_own(2).try_into_resource_any(&mut store)?;
    assert!(owned.resource_drop(&mut store).is_err());
    owned.resource_drop_async(&mut store).await?;

    // A borrow received by an async host import is backed by a host table
    // entry and must be dropped before the import returns; the sync drop is
    // allowed for it on this async-required store.
    let component = Component::new(
        store.engine(),
        r#"
            (component
                (import "t" (type $t (sub resource)))
                (import "peek" (func $peek (param "r" (borrow $t))))
                (core func $peek_lower (canon lower (func $peek)))
                (core module $m
                    (import "" "peek" (func $peek (param i32)))
                    (func (export "run") (param i32)
                        local.get 0
                        call $peek)
                )
                (core instance $i (instantiate $m
                    (with "" (instance (export "peek" (func $peek_lower))))
                ))
                (func (export "run") (param "r" (own $t))
                    (canon lift (core func $i "run")))
            )
        "#,
    )?;
    let seen = Arc::new(AtomicUsize::new(0));
    let mut linker = Linker::new(store.engine());
    linker
        .root()
        .resource("t", ResourceType::host::<u32>(), |_, _| Ok(()))?;
    let peek_seen = seen.clone();
    linker
        .root()
        .func_new_async("peek", move |mut store, _, params, _| {
            let seen = peek_seen.clone();
            Box::new(async move {
                let Val::Resource(borrow) = params[0] else {
                    panic!("expected a resource");
                };
                assert!(!borrow.owned());
                borrow.resource_drop(&mut store)?;
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })?;
    let instance = linker.instantiate_async(&mut store, &component).await?;
    let run = instance.get_typed_func::<(Resource<u32>,), ()>(&mut store, "run")?;
    run.call_async(&mut store, (Resource::new_own(3),)).await?;
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    Ok(())
}
