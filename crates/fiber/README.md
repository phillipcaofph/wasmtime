# Experimental OS-thread-backed execution

## Summary

This is an experimental **OS-thread-backed implementation of Wasmtime's fiber
abstraction**, based on upstream `main` at `73b04cff3317`. It explores execution
on ordinary OS stacks for embedders whose managed runtimes cannot safely run
callbacks on manually switched stacks. It is **not production-ready**, a general
soundness proof, or a guarantee of managed-runtime compatibility.

The backend preserves Wasmtime's cooperative resume/yield/completion interface
but gives each live fiber a dedicated OS worker thread and native stack.
Suspension parks that worker; resumption wakes it while the polling thread
waits for the next yield or completion. The worker stays fixed even when the
polling scheduler changes threads. Teardown joins the worker before releasing
borrowed state.

The `Fiber`, `Suspend` and `StoreFiber` names describe the abstraction, not
lightweight user-space fibers. This is neither a stackless transformation nor
a general thread pool. It is not necessarily one thread per Store, component
instance or explicit guest thread; instantiation can create a cached worker
that later execution reuses.

## Implementation and safety boundaries

`--cfg wasmtime_thread_fibers` selects the adapted thread/condition-variable
backend also used for Miri, without enabling Miri globally. Stock builds retain
their existing stack-switching backend. Ordinary synchronous entrypoints do
not use these workers.

- **Worker handoff:** an owned `Waker` crosses the synchronized handshake,
  rather than a pointer to the poller's non-`Send` `Context`. Each host-future
  poll builds a local context.
- **Activation TLS and GC:** suspension detaches the worker's Wasmtime
  activation list and publishes it to `StoreFiber` for parked-stack tracing.
  Resume and cancellation restore it on that same worker before continuing or
  unwinding. The poller's activation TLS is left alone; worker teardown requires
  empty activation TLS.
- **Accessor TLS:** concurrent host-future polling creates a fresh scoped
  accessor on the executing thread. Sync-lowered imports receive their first
  poll on the guest worker; pending concurrent host futures can later be polled
  on scheduler threads. Accessor TLS is not transported with activation TLS.
  Different Stores may nest event loops; recursive same-Store execution remains
  unsupported.
- **Transfer contracts:** the safe low-level constructor requires `Send`,
  owned `'static` captures and messages. Wasmtime's borrowed startup uses an
  explicitly unsafe constructor, retaining lifetime, transfer and exclusive
  access obligations. Internal unchecked core/component startup must satisfy
  those obligations.
- **Store data:** experimental async instantiation requires `Send` data through
  `AsyncStoreData`, even for locally polled futures. Existing async calls and
  concurrent worker paths require `Send`. Synchronous non-`Send` Stores and the
  stock backend's locally polled non-`Send` async instantiation remain supported.
- **Cleanup:** dropping an unstarted fiber joins its worker and destroys captures
  without executing its body. Failed worker creation returns the original stack
  and drops captures. An unstarted capture-destructor panic is propagated to the
  dropping thread after joining and reclaiming shared state.
- **Configuration:** Engine validation rejects pooling when component-model
  async execution is enabled, and rejects custom stack creators before
  allocator construction. Synchronous configurations can use pooling with
  component-model async disabled. OS-owned stacks do not support raw/custom
  stack allocation, actual stack-bound/guard-range reporting or active
  protection keys. Experimental async execution requires `std`.

Managed callbacks reached on a worker execute there, not on the original
managed/UI thread. There is no callback queue. Arbitrary host TLS and thread
affinity must not be assumed to follow execution.

## Latest validation results

These results are from the latest native rerun on the upstream base above,
using **Rust 1.97** and the experimental cfg unless marked as stock. They are
selected regression results, not a full workspace test or compatibility matrix.

| Check | Linux Arm64 (Docker) | Linux x64 (Docker emulation) | macOS Arm64 |
| --- | --- | --- | --- |
| Wasmtime `thread_tests`, including copying GC | 66/66 passed | 66/66 passed | 66/66 passed |
| Experimental `AsyncStoreData` compile-fail doctests | 4/4 passed | Not rerun | 4/4 passed |
| Low-level fiber constructor compile-fail doctests | 3/3 passed | 3/3 passed | 3/3 passed |
| Low-level fiber unit tests | 11/11 passed | 11/11 passed | 11/11 passed |
| Stock non-`Send` async compatibility doctest | 1/1 passed | Not rerun | 1/1 passed |
| Stock caller-backtrace regression | 1/1 passed | Not rerun | 1/1 passed (upstream unwind exemption) |
| Stock configuration controls | Not rerun | Not rerun | 3/3 passed |

The 66-test suite includes 38 P3 cases, 11 panic/unwind cases, configuration
checks and worker/accessor/parked-root regressions. macOS component fixtures
explicitly disable Mach ports and use Unix-signal exception handling. Windows,
native x64 hardware and the managed .NET stress matrix have not been rerun.
Docker x64 emulation does not replace native x64 validation. No Miri, sanitizer
or performance result is claimed.

Backtrace expectations are backend-specific. Stock builds retain
`backtrace_traces_to_host` and its existing platform exemptions, including
macOS Arm64. Experimental builds run `backtrace_traces_worker_not_poller`,
which requires worker frames and excludes poller frames before and after two
suspensions, while checking worker thread identity. Passing this test does not
provide native caller-backtrace preservation: the worker's native stack does
not contain the poller's call frames. Guest Wasm trap attribution is tested
separately. The worker-backtrace regression also passed a macOS Arm64 release
build, checking that the expected worker frame survives optimization.

### Regression coverage

- [Worker and accessor tests](../wasmtime/src/runtime/component/concurrent/thread_tests.rs)
  cover polling-thread migration, activation/accessor TLS restoration,
  cancellation, parked guest GC roots and traps with pending sibling tasks.
  Deferred reference counting and moving copying GC are exercised; the null
  collector is a non-collecting control. The parked-root workloads verify
  distinct worker IDs and resumed guest values after collection.
- [P3 tests](../wasmtime/src/runtime/component/concurrent/p3_thread_tests.rs)
  cover selected future/stream reads, writes, partial progress, backpressure,
  synchronous and asynchronous cancel-read/cancel-write, resource destructors
  and GC while a guest stack is parked. Additional cases cover owned-resource
  streams, guest-instance stream-reader forwarding, guest-to-guest resource
  transfer and zero-length readiness without consuming the next item.
  Payloads, ABI return codes and exactly-once future destruction are checked.
- [Panic tests](../wasmtime/src/runtime/component/concurrent/panic_thread_tests.rs)
  cover core/component host callbacks, migrated host-future polling,
  cancellation `Drop`, resource-destructor panics and parked sibling guests.
  Typed panic payloads retain their marker and origin thread through
  `catch_unwind`. The parked-sibling case requires three distinct execution
  workers and three nonempty activation-list suspensions.
- [Configuration tests](../wasmtime/src/config/thread_tests.rs) check exact
  rejection errors, that unsupported custom allocators are not invoked, and
  synchronous/async execution with on-demand allocation.
- [Low-level tests](src/lib.rs) cover resume/yield values, cross-thread
  resumption, panic propagation, worker joining, injected spawn failure and
  unstarted capture-destructor panic cleanup.

Isolated lifecycle cases use subprocess timeouts, worker counters and fresh
healthy-Store controls to detect leaks or contaminated polling-thread state.
Failed Stores are not reused. Selected parked-stack cases require nonempty
detached activation lists and traced guest roots.

The future-write cancellation/retry regression expects retry rejection with
`cannot write to future after previous write succeeded or readable end dropped`.
It documents the current behavior rather than claiming future-write retry
support or changing unrelated canonical-ABI semantics.

## Reproducing

Use Rust 1.97 or newer and Wasmtime's native build prerequisites. From the
repository root, set both flags; rustdoc does not inherit `RUSTFLAGS`.

```sh
export RUSTFLAGS="--cfg wasmtime_thread_fibers --check-cfg=cfg(wasmtime_thread_fibers)"
export RUSTDOCFLAGS="$RUSTFLAGS"

cargo test --locked -p wasmtime --lib --features gc-copying thread_tests -- --test-threads=1
cargo test --locked -p wasmtime --doc AsyncStoreData
cargo test --locked -p wasmtime-internal-fiber --features std --lib
cargo test --locked -p wasmtime-internal-fiber --features std --doc
```

The latest Docker runs used `rust:1.97-bookworm`, `--offline --locked` with a
populated Cargo registry, and `--platform linux/arm64` or `linux/amd64`.
macOS runs used the native Arm64 Rust 1.97 toolchain with `--offline --locked`.

For stock-backend compatibility, omit the experimental cfg and use a separate
target directory:

```sh
RUSTFLAGS="--check-cfg=cfg(wasmtime_thread_fibers)" \
RUSTDOCFLAGS="--check-cfg=cfg(wasmtime_thread_fibers)" \
CARGO_TARGET_DIR=target-stock \
cargo test --locked -p wasmtime --doc AsyncStoreData
```

To build an experimental C API library with the flags above:

```sh
cargo build --locked --release -p wasmtime-c-api
```

Diagnostic exports are experimental, not stable public C API:

- `wasmtime_thread_fiber_started`: cumulative started execution workers.
- `wasmtime_thread_fiber_live`: currently live execution workers.
- `wasmtime_thread_fiber_tls_suspensions`: suspensions publishing nonempty
  detached activation lists.

## Remaining limitations

- **Soundness:** `RawFiber` and activation-state `Send` implementations remain
  unsafe. `Send` Store data does not prove every borrowed lifetime, aliasing,
  memory-model, nested Rust-frame, host TLS or FFI obligation. A focused source
  review found no additional concrete violation in the traced paths, not a
  general proof.
- **Coverage:** selected P3 and sibling-task tests do not establish every ABI,
  callback-style export or nested cross-fiber call chain. Resource-destructor
  cancellation/error tests prove future destruction, not successful release of
  an embedder's external resources or delivery to an external sink.
- **Unwinding:** no guarantee is established for `panic=abort`, a second panic
  during active unwinding, panicking hooks/payload destructors, or an unstarted
  capture destructor panicking while the caller is already unwinding.
  Injected spawn failure does not validate actual thread exhaustion or OOM.
- **Stacks and backtraces:** pooling/custom stack allocation and native poller
  backtraces remain unsupported. Backend-specific tests validate worker
  backtraces without claiming reconstruction of the poller's stack.
- **Platforms and managed runtimes:** macOS exception handling, Windows and
  native x64 need broader validation. Native macOS Unix-signal regressions do
  not establish managed-runtime compatibility. Default macOS Mach exception
  handling with CoreCLR remains an unresolved compatibility risk; this
  experiment does not change the production default.
- **Cost:** each live fiber retains an OS thread and stack. Thread creation,
  synchronization, memory use and sustained concurrency costs are unmeasured;
  worker reuse does not remove the thread needed by each parked stack.

Resolving these boundaries and obtaining broader validation are prerequisites
for proposing this as a supported backend.
