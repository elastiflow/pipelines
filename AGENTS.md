# AGENTS — *Guidelines for contributing to* `pipelines`

This document is a playbook for AI/code agents and humans working on the **`github.com/elastiflow/pipelines`** repository. It explains how the codebase is organized, what standards to follow, and how to extend it safely.

---

\<general\_rules>

* **Toolchain & Versions**

    * This module targets **Go `1.23`** and sets a **toolchain `go1.24.2`** in `go.mod`. Use the pinned toolchain to build and test.

        * See `go.mod`:

          ```go
          module github.com/elastiflow/pipelines
          go 1.23.0
          toolchain go1.24.2
          ```
    * Always run locally before a PR:

        * **Format:** `go fmt ./...`
        * **Vet:** `go vet ./...`
        * **Staticcheck:** `staticcheck ./...` (or `make staticcheck`)
        * **Tests (race & coverage):** `go test -race -cover ./...`
        * Keep **zero lints** and **failing tests = 0**.
    * Keep the module tidy: `go mod tidy` (or `make tidy`).

* **Layering & Boundaries**

    * **Root package** (`pipelines/`): Orchestrates a pipeline (glues sources → stream stages → sinks). Provides `Pipeline[T,U]`, `Pipelines[T,U]`, and helpers (`Start`, `Process`, `Broadcast`, `Copy`, `ToSource`, etc.). See `pipeline.go`.
    * **Data streams** (`datastreams/`): Core concurrency primitives & stream operators on **generic** `DataStream[T]`.

        * Operators include `Map`, `Expand`, `Filter`, `Run` (process-in-place), `FanOut`, `FanIn`, `Broadcast`, `OrDone`, `Listen`, `Tee`, `Take`.
        * Error taxonomy lives here (`errors.go`), plus params that control behavior (buffering, fan-out degree, error handling).
    * **Sources** (`datastreams/sources/`): Adapt external producers into `Sourcer[T]` (e.g., `FromArray`, `FromChannel`, `FromDataStream`, `NewEventSourcer`).
    * **Sinks** (`datastreams/sinks/`): Adapt consumers to `Sinker[T]` (e.g., `ToChannel`, `NewBatchSinker`, `ToEventProducer`).
    * **Windowing** (`datastreams/windower/`): Time/window partitioners and logic (`Tumbling`, `Sliding`) that work with `KeyBy` and `Window`.
    * **Internal plumbing** (`datastreams/internal/pipes`): Channel fan-in/out helpers. **Do not** import from outside `datastreams`.

* **When adding code, put it in the right place**

    * **New stream operator** (transform/filter/fan-\*): add to **`datastreams`** (`datastream.go`) with generic signatures and tests.
    * **New source adapter**: add a file in **`datastreams/sources/`** implementing `Sourcer[T]`.
    * **New sink adapter**: add a file in **`datastreams/sinks/`** implementing `Sinker[T]`.
    * **New windowing strategy/partitioner**: add to **`datastreams/windower/`** and wire via `datastreams.Window`.
    * **Keyed state/partition stores**: extend `ShardedStore`/partition APIs in `datastreams/` (not in `internal/`).
    * **Examples** that double as docs: add `example_*.go` at repo root or under the relevant package.

* **Error handling**

    * **Never `panic` in library code.** Operators return `(val, error)` where appropriate and forward typed errors to the pipeline’s error channel.
    * Use the error taxonomy from `datastreams/errors.go`:

        * Codes: `RUN`, `FILTER`, `MAP`, `SINK`, `EXPAND`, `KEY_BY`, `WINDOW`.
        * Helpers: `IsRunError`, `IsFilterError`, `IsMapError`, `IsSinkError`, `IsExpandError`, `IsKeyByError`, `IsWindowError`.
    * Label stages with `datastreams.Params{SegmentName: "my-stage"}` so errors carry useful context.
    * If you need to skip on error, set `Params.SkipError = true` in the relevant operator call.

* **Concurrency & Contexts**

    * **Every pipeline is context-driven.** `pipelines.New` takes a `context.Context` and cancels via `Close()`.
    * **WaitGroup discipline:** `Pipeline.Wait()` blocks for all attached stages. When writing operators, use `WithWaitGroup` and ensure every goroutine `Done()`s.
    * **Channel buffers:** tune via `Params.BufferSize` (operators) or `sources.Params.BufferSize` (sources). Avoid unbuffered channels in hot paths.
    * **Fan-out semantics:**

        * `FanOut(Num=N)`: round-robin distribution among `N` outputs.
        * `Broadcast(Num=N)`: duplicate each item to all `N` outputs.
        * `Listen(i)`: subscribe a new stream to the *i*-th input channel.
        * `FanIn()`: merge multiple inputs into one.

* **Performance & Memory**

    * Prefer passing large payloads by pointer (`*T`) in user code; the library supports both.
    * Use `Params.BufferSize` to minimize blocking; avoid global locks—use per-channel fan-out or `ShardedStore` (`shard.go`) for shared state.
    * Windowers use pooling in hot paths (e.g., sliding batches). Preserve these optimizations when editing.

* **Testing & CI discipline**

    * Favor **fast unit tests** for operator logic and **example-based tests** for docs (`example_*.go` with `Example_*` funcs).
    * Benchmarks live in `/bench` and `datastreams/windower/*_benchmarks_test.go`. Run with `go test -bench=.`.
    * Keep tests deterministic and context-bounded; avoid sleeps unless part of the API contract (windowing, throttled sources).

* **Dependency hygiene**

    * Add dependencies sparingly. Tests use `stretchr/testify`.
    * Keep `go.mod` tidy (`make tidy`). Prefer standard library over third-party where possible.
    * Do **not** depend on `datastreams/internal/pipes` outside `datastreams`.

* **Security & Scans (optional, if tools installed)**

    * Makefile targets:

        * SBOM: `make scan:sbom` (Trivy CycloneDX)
        * SCA: `make scan:sca` (Trivy FS)
        * SAST: `make scan:sast` (Semgrep + gosec)
        * License scan: `make scan:license` (Trivy license)
    * These are **recommended**, not required for local dev.

\</general\_rules>

---

\<repository\_structure>

* **Root**

    * `pipeline.go` — `Pipeline[T,U]`, `Pipelines[T,U]`, orchestration helpers.
    * `doc.go` — package docs with a full example.
    * `README.md`, `CONTRIBUTING.md`, `LICENSE`
    * `go.mod` — module name, Go/toolchain versions, deps.
    * `Makefile` — includes `mk/go.mk`; convenience targets for tidy, tests, staticcheck, scans, docs.
    * `scripts/test_coverage_threshold.py` — checks coverage threshold for packages.
    * `bench/` — microbenchmarks.
    * `example_*.go` — runnable examples that serve as docs.

* **`datastreams/` (core)**

    * `datastream.go` — `DataStream[T]` and operators: `Map`, `Expand`, `Filter`, `Run`, `FanOut`, `FanIn`, `Broadcast`, `OrDone`, `Listen`, `Tee`, `Take`, `Sink`, etc.
    * `types.go` — function types (`TransformFunc`, `ProcessFunc`, `FilterFunc`, `ExpandFunc`, `WindowFunc`), keyed/window interfaces, shard types.
    * `params.go` — `Params` for operators (`Num`, `BufferSize`, `SkipError`, `SegmentName`, `ShardCount`).
    * `errors.go` — error codes, constructors, and predicate helpers.
    * `keyed.go` — `KeyBy`, `KeyedDataStream`, `Window`, timed/keyable element utilities.
    * `shard.go` — `ShardedStore` and shard key functions.
    * `sourcer.go` / `sinker.go` — interfaces.

* **`datastreams/sources/`**

    * `array.go` — `FromArray(slice, sources.Params{Throttle?, BufferSize?})`
    * `channel.go` — `FromChannel(<-chan T)`
    * `datastream.go` — `FromDataStream(ds)` for piping a pipeline into another as a source.
    * `eventsourcer.go` — `NewEventSourcer(streamSize, consumer)` for queue-like consumers; interfaces:

        * `EventConsumer[T]` (composite of `Runner`, `MessageReader[T]`, `MessageMarker[T]`, `ErrorMarker[T]`).
    * `params.go` — `sources.Params` (`BufferSize`, `Throttle`).

* **`datastreams/sinks/`**

    * `channel.go` — `ToChannel(chan<- T)`
    * `batch_sinker.go` — `NewBatchSinker(onFlush func(ctx, []T) error, batchSize int, errs chan error)`
    * `eventsinker.go` — `ToEventProducer(producer)` where `producer` implements `EventProducer[T]`.

* **`datastreams/windower/`**

    * `tumbling.go` — `NewTumbling[T,K](windowDuration time.Duration)`
    * `sliding.go` — `NewSliding[T,K](windowDuration, slideInterval time.Duration)`
    * Benchmarks and tests.

* **`datastreams/internal/pipes/`**

    * `types.go` — multi-channel sender/receiver helpers. **Private**.

\</repository\_structure>

---

\<core\_concepts\_and\_api\_cheatsheet>

* **Create a pipeline**

  ```go
  errCh := make(chan error, 16)
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

  pl := pipelines.New[int,string](
      ctx,
      sources.FromArray([]int{1,2,3,4,5}),
      errCh,
  ).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[string] {
      return datastreams.Map[int,string](ds, func(v int) (string, error) {
          return fmt.Sprintf("v=%d", v), nil
      })
  })
  ```

* **Consume output / handle errors**

  ```go
  go func() {
      for err := range pl.Errors() {
          // classify with datastreams.IsMapError/IsFilterError/... if needed
          log.Println("stage error:", err)
      }
  }()
  for s := range pl.Out() { fmt.Println(s) }
  ```

* **In-place processing on source (`Run`)**

  ```go
  pl = pl.Process(func(v int) (int, error) {
      if v%2==0 { return v*2, nil }
      return v, nil
  }, datastreams.Params{SegmentName: "double-evens"})
  ```

* **Fan-out vs broadcast**

  ```go
  // N workers with disjoint items (round-robin)
  workers := pl.Copy(4).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
      return ds.FanOut(datastreams.Params{Num: 4})
  })

  // N pipelines each see every item
  clones := pl.Broadcast(3, func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
      return ds // any per-clone ops
  })
  ```

* **Sinks**

  ```go
  // To channel
  out := make(chan string, 32)
  _ = pl.Sink(sinks.ToChannel(out))

  // Batch sink
  bs := sinks.NewBatchSinker(func(ctx context.Context, batch []string) error {
      // flush batch
      return nil
  }, 200, errCh)
  _ = pl.Sink(bs)
  ```

* **Event sources / sinks**

  ```go
  // Source from a queue-like consumer
  src := sources.NewEventSourcer[int](1024, myConsumer)
  pl := pipelines.New[int,int](ctx, src, errCh).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
      return ds.Run(func(v int) (int,error){ return v*v, nil })
  })

  // Sink to a producer (e.g., message bus)
  _ = pl.Sink(sinks.ToEventProducer(myProducer))
  ```

* **Keyed windowing**

  ```go
  // Key by DeviceID and compute a tumbling window
  part := windower.NewTumbling[*Reading, string](250*time.Millisecond)
  keyed := datastreams.KeyBy[*Reading,string](ds, func(r *Reading) string { return r.DeviceID })

  out := datastreams.Window[*Reading,string,*Result](
      keyed,
      func(win []*Reading) (*Result, error) { /* aggregate */ },
      part,
      datastreams.Params{ShardCount: 64, SegmentName: "tumbling"},
  )
  ```

* **Utilities**

    * `Take(n)` to take first *n* items and close.
    * `OrDone()` to propagate cancellation.
    * `Tee()` to split a stream into two identical downstreams.
    * `Listen(i)` to tap the *i*-th input channel of a multi-input stream.

\</core\_concepts\_and\_api\_cheatsheet>

---

\<how\_to\_extend>

* **Add a new operator (transform/filter/etc.)**

    1. Implement in `datastreams/datastream.go` or a new file in `datastreams/`.
    2. Accept a `Params` parameter (variadic) and call `applyParams`.
    3. Create channels with `next[T](...)` (returns a `DataStream[T]` and channel set).
    4. Respect `ctx.Done()` and `SkipError`. Label errors with `SegmentName`.
    5. Increment/`Done()` a `WaitGroup` if present (via `WithWaitGroup` on incoming `DataStream`).
    6. Close all produced channels on exit.
    7. Add tests in `datastreams/*_test.go` and an `Example_*` if the API is public.

* **Add a new source**

    1. Implement a `type X[T any] struct{ ... }` in `datastreams/sources/` holding a buffered output channel and any config (`sources.Params`).
    2. Implement `Source(ctx, errSender) datastreams.DataStream[T]`.
    3. Launch a goroutine to publish to the output channel; `defer close(out)`.
    4. Respect throttling (`sources.Params.Throttle`) if applicable.

* **Add a new sink**

    1. Implement `Sink(ctx, ds datastreams.DataStream[T]) error` in `datastreams/sinks/`.
    2. Read from `ds.Out()`; observe `ctx.Done()` to exit gracefully.
    3. For async flushers (like `BatchSinker`), use internal goroutines and error channel for failures.

* **Add a new windowing strategy**

    1. Add a partitioner struct in `datastreams/windower/` implementing `datastreams.Partitioner[T,K]`.
    2. Your `Create(ctx, out)` returns a `Partition[T,K]` that buffers items and emits `[]T` to `out` as windows.
    3. Provide `Close()` semantics to flush pending batches and stop timers.
    4. Ensure thread-safety for partition state; use `sync.Mutex`/`sync.WaitGroup` as needed.

\</how\_to\_extend>

---

\<dependencies\_and\_installation>

* **Prerequisites**

    * **Go**: 1.23+ with support for the **toolchain directive** (the repo requests `go1.24.2`).
    * **staticcheck** for linting (install via `go install honnef.co/go/tools/cmd/staticcheck@latest`).
    * Optional:

        * **trivy** (SBOM/SCA/license scans)
        * **semgrep** and **gosec** (SAST)
        * **godoc** (local docs server)

* **Install & Build**

  ```bash
  go mod download
  go mod tidy
  go build ./...
  ```

* **Format, Lint, Vet**

  ```bash
  go fmt ./...
  go vet ./...
  staticcheck ./...
  # or
  make tidy
  make staticcheck
  ```

* **Docs (local)**

  ```bash
  make docs         # runs: godoc -http=:6060
  # open http://localhost:6060/pkg/github.com/elastiflow/pipelines/
  ```

\</dependencies\_and\_installation>

---

\<testing\_instructions>

* **Run all tests**

  ```bash
  go test ./...
  go test -race ./...
  ```

* **Coverage**

  ```bash
  go test -cover ./...
  # HTML coverage for a pkg
  go test -coverprofile=cover.out ./datastreams && go tool cover -html=cover.out
  ```

* **Make targets** (when available on your system)

  ```bash
  make test-unit              # unit tests (writes coverage data under mk/coverage/)
  make test-unit-verbose
  make staticcheck
  make tidy
  ```

* **Benchmarks**

  ```bash
  go test -bench=. -benchmem ./bench
  go test -bench=. -benchmem ./datastreams/windower
  ```

* **Coverage thresholds** (optional)

    * Script: `scripts/test_coverage_threshold.py`
    * Example:

      ```bash
      python3 scripts/test_coverage_threshold.py \
        --makefile Makefile \
        --threshold 60.0 \
        --go-mods github.com/elastiflow/pipelines github.com/elastiflow/pipelines/datastreams
      ```

* **What to test**

    * Operator behavior (success + error paths, `SkipError`, buffering).
    * Context cancellation handling (`OrDone`, `Close`, `Wait`).
    * Fan-in/out correctness (no deadlocks; channels closed).
    * Windowing emissions (`Tumbling`, `Sliding`) and keyed routing (`KeyBy`).
    * Sharded state (`ShardedStore`) concurrency semantics.

\</testing\_instructions>

---

\<style\_and\_docs>

* **Public API docs**

    * Add package and exported symbol comments. See `doc.go` for style.
    * Prefer **`Example_*`**-style tests (`example_*.go` at root) to document end-to-end flows.

* **Coding style**

    * Idiomatic Go: short names for short scopes; avoid unnecessary abstractions.
    * Keep files focused; split large operators into helper functions when they grow.
    * Avoid global mutable state; prefer `context.Context` + channels.

* **Logging**

    * The library itself avoids logging. Surface issues via **errors** on the pipeline error channel.
    * Examples may use `log/slog`, but **do not** bake logging into operators.

\</style\_and\_docs>

---

\<gotchas\_and\_tips>

* **Use `SegmentName`** in `datastreams.Params` to make error messages useful:

  ```go
  datastreams.Map(ds, fn, datastreams.Params{SegmentName: "parse-json"})
  ```
* **`SkipError`** controls whether an item is dropped on operator error. If `false`, ensure your operator does not emit a zero-value unintentionally.
* **`BufferSize`** matters: under-buffered channels cause contention; over-buffering increases memory.
* **`FanIn`** closes outputs once all inputs are drained—ensure every input goroutine terminates.
* **Windowing** often involves timers; always respect `ctx.Done()` and close timers on `Close()`.

\</gotchas\_and\_tips>

---

\<license\_and\_attribution>

* The project is licensed under **Apache 2.0** (`LICENSE`).
* Module: `github.com/elastiflow/pipelines`.

\</license\_and\_attribution>
