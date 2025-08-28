Project: github.com/elastiflow/pipelines

This document captures project-specific development notes for advanced contributors. It focuses on the concrete tooling and flows used in this repo (Go 1.23+), including build/config, testing, coverage, and useful conventions.

1. Build and configuration
- Go toolchain: go 1.23 with toolchain directive go1.24.2 in go.mod. Install Go >= 1.23; the toolchain will auto-download 1.24.2 if your Go supports toolchains.
- Module layout: Single module at the repo root (module github.com/elastiflow/pipelines). Some directories do not contain tests and are meant for internal types and plumbing.
- Makefile integration: The root Makefile includes mk/go.mk with standardized targets. The Make targets iterate over all discovered go.mod files, but this repo currently has a single module.
- External tooling used by targets (install if you intend to run them):
  - staticcheck (golang.org/x/tools/staticcheck)
  - trivy (for SBOM/license/SCA scans)
  - semgrep, gosec (for SAST)
  - godoc (for docs target)

Key make targets:
- make test-unit: Runs unit tests for all packages with -race and -cover, and writes coverage using -test.gocoverdir under coverage/unit.
- make test-unit-verbose: Same as above with -v.
- make test-unit-report-coverage: Prints coverage functions summary for a specific module subset via GO_TEST_COVERAGE_MOD.
- make test-unit-report-coverage-html: Emits a combined coverage.out and HTML report in coverage/.
- make test-unit-enforce-coverage: Enforces a minimum coverage threshold across discovered modules via scripts/test_coverage_threshold.py. Threshold defaults to 0; override GO_TEST_COVERAGE_THRESHOLD to enforce.
- make tidy: Runs go mod tidy in the module(s).
- make staticcheck: Runs staticcheck ./... across module(s).
- make docs: Runs a local godoc server on :6060.

Environment/config knobs:
- GO_TEST_OPTS can pass additional flags to go test via the make targets.
- GO_TEST_COVERAGE_MOD scopes coverage reporting/HTML to a subdirectory (e.g., datastreams/windower). Slashes are converted to _ in the output filename.
- GO_TEST_COVERAGE_THRESHOLD controls the threshold in test-unit-enforce-coverage.
- GO_TEST_EXCLUDE_MOD_REGEX controls which go.mod locations are excluded if the repo grows into a multi-module layout.

2. Testing information
Running tests directly:
- go test ./...  — runs all tests. Verified to pass with race disabled.
- make test-unit  — preferred CI-like run: adds -race and coverage collection. Verified to pass locally; produces per-package coverage and writes covdata under coverage/unit.

Focused runs and options:
- Run a single package: go test ./datastreams
- Run matching tests only: go test ./datastreams -run "^TestShard|TestJumpHash$"
- Verbose: append -v, or use make test-unit-verbose.
- Race detector: use make test-unit (already enabled) or go test -race ./...
- Benchmarks: packages include benchmarks (e.g., datastreams/windower). Run via: go test -bench=. -benchmem ./datastreams/windower

Coverage reporting and enforcement:
- Generate function-level coverage summary for a subset:
  make test-unit && \
  make test-unit-report-coverage GO_TEST_COVERAGE_MOD=datastreams
- Generate HTML report:
  make test-unit && \
  make test-unit-report-coverage-html GO_TEST_COVERAGE_MOD=datastreams/windower
  Output: coverage/<mod>.out and coverage/<mod>.html (mod path slashes become underscores). Open the .html in a browser.
- Enforce coverage threshold (example 85% for root module):
  make test-unit GO_TEST_OPTS="-count=1" && \
  make test-unit-enforce-coverage GO_TEST_COVERAGE_THRESHOLD=85
  The enforcement script enumerates modules and prints pass/fail per module; it also generates the HTML reports as part of its workflow.

Adding a new test (guidance):
- Place tests alongside the code in the same package, as *_test.go files. Prefer table-driven tests and use github.com/stretchr/testify for assertions if suitable (already required in go.mod). Example patterns are prevalent in existing *_test.go files across datastreams, sources, sinks, and windower.
- Name long-running/integration tests with prefixes TestIntegration or TestE2E if you want them skipped by make test-unit (the make target skips ^(TestIntegration|TestE2E).* by default). This prevents slowing down unit runs while still allowing explicit runs via -run.
- Concurrency-sensitive code: Many components are concurrent (e.g., ShardedStore backed by sync.Map). When adding tests involving concurrency, run with -race locally and consider deadlines/timeouts for channel operations to avoid hangs in CI.

Demonstration test process (commands we validated):
- Create a trivial test in an existing package, then run it in isolation:
  1) Create datastreams/guidelines_demo_test.go with:
     package datastreams
     import "testing"
     func TestDemo_Addition(t *testing.T) {
         if 2+2 != 4 { t.Fatal("math is broken") }
     }
  2) Run: go test ./datastreams -run TestDemo_Addition -v  (PASS)
  3) Remove the file after verification to keep the repo clean.
- Full suite sanity check:
  - go test ./...
  - make test-unit
  Both have been verified to pass on this codebase at the time of writing, with coverage collected under coverage/unit.

3. Additional development information
Code style and linting:
- Use go fmt (or goimports) before committing; CI-focused staticcheck target is available: make staticcheck.
- Keep APIs and generics simple; the project already uses generic types (e.g., ShardedStore[T, K comparable]). Prefer explicit type parameters and constraints for readability.

Sharding/hash utilities:
- datastreams/shard.go provides two shard key functions: ModulusHash and JumpHash. ModulusHash remaps most keys when shard count changes; JumpHash minimizes remapping. If you introduce new storage keyed by shard, prefer JumpHash for dynamic shard counts.
- ShardedStore opts default ShardCount to 1 if zero is passed. Ensure callers set meaningful shard counts and a ShardKeyFunc.
- store.Initialize pre-allocates sync.Map shards; Set lazily initializes missing shards. In high-throughput scenarios, prefer calling Initialize on a new ShardedStore before first use.

Testing patterns in this repo:
- Extensive example_*_test.go files demonstrate public API usage and serve as executable documentation. When changing behavior, update examples accordingly because they are compiled and often executed in tests.
- Benchmarks exist under bench/ and windower/. Keep benchmark inputs small and deterministic.

Docs:
- Run make docs to spin up a local godoc server on :6060 for API browsing during development.

Security/tooling scans (optional in local dev):
- SBOM: make scan:sbom (requires trivy)
- SCA: make scan:sca (requires trivy)
- SAST: make scan:sast (requires semgrep and gosec)
- Licenses: make scan:license (requires trivy)

Notes on CI stability:
- Use -count=1 when troubleshooting flaky tests to bypass test result caching.
- Prefer context with deadlines/timeouts for operations involving channels and goroutines in tests.
- If adding integration/E2E tests, gate them via naming convention and/or build tags so unit pipelines remain fast.
