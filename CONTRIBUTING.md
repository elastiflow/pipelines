# Contributing to Pipelines

Thank you for your interest in contributing to **Pipelines**! We welcome and appreciate contributions from the community. This document will guide you through the steps and best practices to make your contributions as smooth as possible.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Getting the Source](#getting-the-source)
3. [Project Structure](#project-structure)
4. [Development Workflow](#development-workflow)
5. [Coding Guidelines](#coding-guidelines)
6. [Testing and Coverage](#testing-and-coverage)
7. [Documentation](#documentation)
8. [Submitting Changes](#submitting-changes)
9. [Reporting Issues](#reporting-issues)
10. [Code of Conduct](#code-of-conduct)
11. [License](#license)

---

## Prerequisites

- Latest stable version of Go installed on your system.
- A GitHub account to [submit issues](https://github.com/elastiflow/pipelines/issues) or [create pull requests](https://github.com/elastiflow/pipelines/pulls).

---

## Getting the Source

1. [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) this repository into your personal GitHub account.
2. Clone your fork locally:

   ```bash
   git clone https://github.com/<your-username>/pipelines
   cd pipelines
   ```

3. Add the main repo as a remote, so you can keep it updated:

   ```bash
   git remote add upstream https://github.com/elastiflow/pipelines.git
   ```

4. Always work on a branch (not `main`). Keep `main` in sync with upstream:

   ```bash
   git checkout main
   git pull upstream main
   git checkout -b feature/my-new-feature
   ```

---

## Project Structure

```
pipelines/
├── bench/                # Benchmark tests
├── datastreams/          # Core pipeline primitives and transformations
│   ├── sources/          # Source implementations
│   └── sinks/            # Sink implementations
├── examples/             # Example programs demonstrating usage
├── pipelines/            # Primary pipeline package
├── go.mod / go.sum       # Go module files
├── doc.go                # Package-level documentation
├── pipeline.go           # Main pipeline definition
├── pipeline_test.go      # Integration tests for pipeline
├── README.md             # High-level overview and usage
└── CONTRIBUTING.md       # (This file)
```

---

## Development Workflow

1. **Create or update your local branch** off the latest `main`.
2. Make changes to code or documentation.
3. **Lint and format** your code:
   ```bash
   go fmt ./...
   go vet ./...
   ```
4. **Run tests** to ensure nothing is broken:
   ```bash
   go test ./... -v
   ```
5. **Check coverage** (we aim for 85%+ coverage):
   ```bash
   go test ./... -cover
   ```
6. Commit your changes with a clear commit message:
   ```bash
   git commit -m "Add new feature X with unit tests"
   ```
7. Push your branch to GitHub:
   ```bash
   git push origin feature/my-new-feature
   ```
8. Open a **Pull Request** from your fork/branch into `elastiflow/pipelines:main`.

---

## Coding Guidelines

- **Go Version**: Target the latest stable Go version.
- **Style**:
    - Use `go fmt` for formatting.
    - `staticcheck` for linting.
    - Follow [Effective Go](https://go.dev/doc/effective_go) and standard Go conventions.
- **Naming**: Use descriptive, concise names. Public symbols should be documented (commented).
- **Error Handling**: Always handle errors; return them up or log them appropriately.
- **Concurrency**: Pipelines heavily rely on channels & goroutines. Ensure no accidental leaks or deadlocks.

---

## Testing and Coverage

- **Write tests** for any changes or new features.
- Place unit tests in `_test.go` files in the same package.
- We use [Testify](https://github.com/stretchr/testify) for convenient assertions in many places.
- Run the full test suite:

  ```bash
  go test -v ./...
  ```

- **Increase coverage** for new and existing code. Run with coverage:

  ```bash
  go test -cover ./...
  ```

- **Integration Tests**: For end-to-end testing of the pipeline behavior, we rely on `pipeline_test.go` and other test files that coordinate multiple pipeline stages.

---

## Documentation

- **Inline Comments**: Use standard Go doc comments on all public functions, types, and packages.
- **Examples**: If adding a new feature, please add or update an example in `./examples` or in the doc comments where relevant.
- **Regenerating/Viewing Docs**:
  ```bash
  go install golang.org/x/tools/cmd/godoc@latest
  make docs # or go run ...
  # Then open http://localhost:6060/pkg/github.com/elastiflow/pipelines/
  ```

---

## Submitting Changes

1. Ensure your local branch is rebased onto `main`:
   ```bash
   git pull --rebase upstream main
   ```
2. Push your commits to GitHub:
   ```bash
   git push origin feature/my-new-feature
   ```
3. Create a **Pull Request** against the `elastiflow/pipelines:main` branch:
    - Provide a clear description of the issue or feature, any special instructions for testing, and relevant context.
    - The CI checks will run automatically.
4. Address any review feedback from maintainers.
5. Once approved, your PR will be merged. You may delete your local branch afterward.

---

## Reporting Issues

If you find a bug or have an enhancement request:

1. [Open an Issue](https://github.com/elastiflow/pipelines/issues) in GitHub.
2. Include as many details as possible:
    - Steps to reproduce
    - Relevant logs or error messages
    - Environment details (OS, Go version, etc.)
    - Proposed solutions or workarounds, if any

---

## Code of Conduct

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/) to foster a friendly and harassment-free environment. Please be respectful to others and help us maintain a welcoming, inclusive community.

---

## License

This project is open-sourced under the [Apache License 2.0](LICENSE). By contributing, you agree that your contributions will be licensed under its terms.

---

Thank you for your interest in contributing! We look forward to collaborating with you. If you have any questions or need guidance, don’t hesitate to open an issue or reach out in a pull request. Happy coding!