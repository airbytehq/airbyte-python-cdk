# API Reference

The Airbyte Python CDK provides a comprehensive framework for building source and destination connectors. This API reference is automatically generated from the source code and docstrings.

## Package Overview

The CDK is organized into several main components:

### Core Components
- **Sources** (`airbyte_cdk.sources`) - Base classes and utilities for building source connectors, including:
  - Stream abstractions and implementations
  - HTTP and file-based connectors
  - Authentication handling
  - State management
  - Declarative configuration

- **Destinations** (`airbyte_cdk.destinations`) - Components for building destination connectors:
  - Base classes for implementing destinations
  - Batch processing utilities
  - Error handling

- **Models** (`airbyte_cdk.models`) - Protocol data types and schemas:
  - Configuration models
  - Message formats
  - Protocol definitions

- **Utils** (`airbyte_cdk.utils`) - Common utilities and helpers

## Documentation Structure

The complete module documentation is auto-generated and includes:
- Full class hierarchies and inheritance
- Method signatures with type hints
- Detailed docstrings
- Source code references
- Cross-referenced types

Use the sidebar navigation to explore the full API documentation. The documentation is organized by module, with all public classes, methods, and attributes automatically included.
