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
8. [Cursor Rules](#cursor-rules)
9. [Submitting Changes](#submitting-changes)
10. [Reporting Issues](#reporting-issues)
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
   git clone https://github.com/elastiflow/pipelines.git
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
├── bench/                        # Benchmark tests for performance/regressions
├── datastreams/                  # Core pipeline primitives and transformations
│   ├── sources/                  # Source implementations
│   ├── sinks/                    # Sink implementations
│   └── windower/                 # Sliding/tumbling window operators and benches
├── docs/img/                     # Project images used in docs/README
├── coverage/                     # Test coverage output (generated)
│   └── unit/                     # Go test -coverdata artifacts (generated)
├── mk/go.mk                      # Shared make logic used by root Makefile
├── AGENTS.md                     # Notes for AI/agent-based workflows
├── LICENSE                       # Project license (Apache-2.0)
├── Makefile                      # Build/test/lint targets (see make help)
├── README.md                     # High-level overview and usage
├── doc.go                        # Package-level documentation entrypoint
├── go.mod / go.sum               # Go module files
├── pipeline.go                   # Main pipeline definition
├── pipeline_test.go              # End-to-end/integration tests for pipeline
├── example_*.go                  # Executable documentation via examples
└── CONTRIBUTING.md               # (This file)
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

## Cursor Rules

This project uses Cursor rules to provide AI assistance and maintain consistent development practices. Cursor rules are stored in the `.cursor/rules` directory and are version-controlled alongside your codebase.

### Understanding Cursor Rules

Cursor rules provide system-level instructions to the AI, offering persistent context, preferences, or workflows specific to this project. They help ensure consistent code quality, testing standards, and development practices across all contributors.

### Rule Structure

Each rule is written in MDC (`.mdc`) format and includes:

```yaml
---
description: Brief description of the rule
globs: ["**/*.go"]  # File patterns for auto-attachment
alwaysApply: false  # Whether to always include
---
```

### Rule Types

- **Always Applied**: Core standards that are always included (e.g., Go coding standards)
- **Auto-Attached**: Rules that apply based on file patterns (e.g., datastreams-specific guidance)
- **Agent Requested**: Rules available to AI when needed (e.g., development workflow)
- **Manual**: Rules that must be explicitly invoked (e.g., `@code-review`)

### Creating and Managing Rules

#### Creating a New Rule

1. **Using Cursor Interface**:
    - Use the `New Cursor Rule` command within Cursor
    - Navigate to `Cursor Settings > Rules` to create a new rule file
    - Rules are automatically created in the `.cursor/rules` directory

2. **Manual Creation**:
    - Create a new `.mdc` file in the appropriate `.cursor/rules` directory
    - Follow the MDC format with proper metadata
    - Place rules in the most specific directory for their scope

#### Rule Organization

```
.cursor/rules/                    # Project-wide rules
datastreams/
  .cursor/rules/                  # Datastream-specific rules
  sources/
    .cursor/rules/                # Source-specific rules
  sinks/
    .cursor/rules/                # Sink-specific rules
  windower/
    .cursor/rules/                # Windowing-specific rules
```

#### Generating Rules from Conversations

If you've made decisions about agent behavior or development practices:

1. **Use the `/Generate Cursor Rules` command** within the chat interface
2. **Provide context** about what the rule should cover
3. **Review and refine** the generated rule before committing
4. **Test the rule** in context to ensure it's helpful

### Best Practices for Rules

- **Keep rules focused**: Each rule should cover a specific domain or concern
- **Limit rule size**: Keep rules under 500 lines for clarity
- **Provide examples**: Include concrete examples and code snippets
- **Use clear language**: Write rules as you would clear internal documentation
- **Test in context**: Verify rules work correctly when applied
- **Update regularly**: Keep rules current with codebase changes

### Common Rule Patterns

#### Domain-Specific Rules
```mdc
---
description: Guidance for working with datastreams package
globs: ["**/datastreams/**/*.go"]
alwaysApply: false
---
# Datastream Operations
## Core Concepts
...
```

#### Always Applied Rules
```mdc
---
description: Core Go coding standards
globs: []
alwaysApply: true
---
# Go Coding Standards
## General Principles
...
```

#### Auto-Attached Rules
```mdc
---
description: Testing conventions for benchmarks
globs: ["**/bench/**/*.go", "**/benchmarks_*.go"]
alwaysApply: false
---
# Benchmark Testing
## Structure
...
```

### Maintaining Rules

1. **Review rules regularly** to ensure they remain relevant
2. **Update rules** when coding standards or patterns change
3. **Remove obsolete rules** that are no longer applicable
4. **Test rule effectiveness** by using them in development
5. **Gather feedback** from other contributors on rule usefulness

### Rule Validation

Before committing rule changes:

- [ ] Rule follows MDC format correctly
- [ ] Metadata fields are properly set
- [ ] Content is clear and actionable
- [ ] Examples are accurate and up-to-date
- [ ] Rule has been tested in context
- [ ] Rule size is under 500 lines

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

## License

This project is open-sourced under the [Apache License 2.0](LICENSE). By contributing, you agree that your contributions will be licensed under its terms.

---

Thank you for your interest in contributing! We look forward to collaborating with you. If you have any questions or need guidance, don’t hesitate to open an issue or reach out in a pull request. Happy coding!