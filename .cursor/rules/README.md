# Cursor Rules for Pipelines Project

This directory contains Cursor rules that provide AI assistance for the Go pipelines project. These rules are organized to provide context-aware guidance based on the type of work you're doing.

## Rule Types

### Always Applied Rules
- **go-standards.mdc**: Core Go coding standards and project conventions
- **pipeline-patterns.mdc**: Pipeline architecture and design patterns
- **testing-standards.mdc**: Testing conventions and coverage requirements

### Auto-Attached Rules
- **datastreams.mdc**: Applied when working with datastreams package
- **sources-sinks.mdc**: Applied when working with sources or sinks
- **windowing.mdc**: Applied when working with windowing operations
- **benchmarks.mdc**: Applied when working with benchmark tests

### Nested Auto-Attached Rules
- **`datastreams/.cursor/rules/datastream-operations.mdc`**: Applied when working in datastreams directory
- **`datastreams/sources/.cursor/rules/source-patterns.mdc`**: Applied when working with source implementations
- **`datastreams/sinks/.cursor/rules/sink-patterns.mdc`**: Applied when working with sink implementations
- **`datastreams/windower/.cursor/rules/windowing-patterns.mdc`**: Applied when working with windowing patterns
- **`datastreams/windower/.cursor/rules/windowing-implementation.mdc`**: Applied when working with windowing implementations

### Agent Requested Rules
- **development-workflow.mdc**: Development and contribution guidelines
- **performance-optimization.mdc**: Performance tuning and optimization tips

### Manual Rules
- **code-review.mdc**: Code review checklist and standards
- **documentation.mdc**: Documentation standards and templates

## Nested Rules

Subdirectories contain their own `.cursor/rules` for domain-specific guidance:

### **datastreams/.cursor/rules/**
- **`datastream-operations.mdc`**: Specific guidance for DataStream operations and transformations

### **datastreams/sources/.cursor/rules/**
- **`source-patterns.mdc`**: Implementation patterns and best practices for data sources
- **Covers**: Database sources, file sources, HTTP sources, generator sources, streaming patterns

### **datastreams/sinks/.cursor/rules/**
- **`sink-patterns.mdc`**: Implementation patterns and best practices for data sinks
- **Covers**: Channel sinks, batch sinks, event sinks, custom sink implementations

### **datastreams/windower/.cursor/rules/**
- **`windowing-patterns.mdc`**: High-level windowing patterns and usage guidance
- **`windowing-implementation.mdc`**: Detailed implementation patterns for windowing operations
- **Covers**: Tumbling, sliding, and interval windows with implementation examples

## Usage

Rules are automatically applied based on:
1. **File patterns**: Rules with glob patterns are attached when matching files are referenced
2. **Directory context**: Nested rules apply when working in specific directories
3. **Manual invocation**: Use `@ruleName` to explicitly include a rule

## Rule Format

All rules use the MDC (Markdown with Context) format with metadata headers:

```yaml
---
description: Brief description of the rule
globs: ["*.go", "**/*.go"]  # File patterns for auto-attachment
alwaysApply: false          # Whether to always include
---
```

## Contributing

When adding new rules:
1. Follow the existing naming convention
2. Include clear examples and references
3. Keep rules focused and under 500 lines
4. Test rules in context to ensure they're helpful
