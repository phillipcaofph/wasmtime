use super::*;
use crate::{Engine, Instance, Module, Store};
use core::task::{Context, Poll, Waker};

#[cfg(all(wasmtime_thread_fibers, feature = "pooling-allocator"))]
#[test]
fn thread_stack_config_rejects_pooling_before_allocator_creation() {
    for total_stacks in [0, 1, u32::MAX] {
        let mut pool = PoolingAllocationConfig::default();
        pool.total_stacks(total_stacks);
        let mut config = Config::new();
        config.wasm_component_model_async(true);
        config.allocation_strategy(pool);
        let error = Engine::new(&config)
            .err()
            .expect("pooling must be rejected");
        assert_eq!(
            error.to_string(),
            "thread-backed execution does not support the pooling allocator; use InstanceAllocationStrategy::OnDemand"
        );
    }
}

#[cfg(all(wasmtime_thread_fibers, feature = "pooling-allocator"))]
#[test]
fn thread_stack_config_accepts_pooling_when_async_execution_is_disabled() -> Result<()> {
    let mut pool = PoolingAllocationConfig::default();
    pool.total_stacks(1)
        .total_memories(1)
        .total_tables(1)
        .total_core_instances(1)
        .total_component_instances(1);
    let mut config = Config::new();
    config.wasm_component_model_async(false);
    config.allocation_strategy(pool);
    Engine::new(&config)?;
    Ok(())
}

struct UnexpectedStackCreator;

// This creator never hands out memory. Engine validation must reject it
// without invoking allocation.
unsafe impl crate::StackCreator for UnexpectedStackCreator {
    fn new_stack(&self, _: usize, _: bool) -> Result<Box<dyn crate::StackMemory>, Error> {
        panic!("unsupported custom stack creator was invoked");
    }
}

#[test]
#[cfg(wasmtime_thread_fibers)]
fn thread_stack_config_rejects_custom_creator_before_execution() {
    let mut config = Config::new();
    config.with_host_stack(Arc::new(UnexpectedStackCreator));
    let error = Engine::new(&config)
        .err()
        .expect("custom stacks must be rejected");
    assert_eq!(
        error.to_string(),
        "thread-backed execution does not support custom stack creators; remove Config::with_host_stack"
    );
}

#[cfg(all(not(wasmtime_thread_fibers), feature = "pooling-allocator"))]
#[test]
fn stock_stack_config_still_accepts_pooling() -> Result<()> {
    let mut pool = PoolingAllocationConfig::default();
    pool.total_stacks(1)
        .total_memories(1)
        .total_tables(1)
        .total_core_instances(1)
        .total_component_instances(1);
    let mut config = Config::new();
    config.allocation_strategy(pool);
    Engine::new(&config)?;
    Ok(())
}

#[cfg(not(wasmtime_thread_fibers))]
#[test]
fn stock_stack_config_still_accepts_custom_creator() -> Result<()> {
    let mut config = Config::new();
    config.with_host_stack(Arc::new(UnexpectedStackCreator));
    Engine::new(&config)?;
    Ok(())
}

#[test]
fn thread_stack_config_on_demand_preserves_sync_and_async_execution() -> Result<()> {
    use core::future::Future;
    let mut config = Config::new();
    #[cfg(wasmtime_thread_fibers)]
    config.macos_use_mach_ports(false);
    config.allocation_strategy(InstanceAllocationStrategy::OnDemand);
    let engine = Engine::new(&config)?;
    let module = Module::new(
        &engine,
        "(module (func (export \"answer\") (result i32) i32.const 42))",
    )?;
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[])?;
    let function = instance.get_typed_func::<(), i32>(&mut store, "answer")?;
    assert_eq!(function.call(&mut store, ())?, 42);
    let mut future = Box::pin(function.call_async(&mut store, ()));
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(42))
    ));
    Ok(())
}
