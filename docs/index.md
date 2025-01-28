# Airbyte Python CDK

The Airbyte Python CDK (Connector Development Kit) is a framework for building Airbyte connectors. It provides a set of classes and helpers that make it easy to build connectors against HTTP APIs (REST, GraphQL, etc.) or create generic Python source connectors.

## Overview

The Python CDK simplifies connector development by handling the complexity of implementing the Airbyte protocol, allowing you to focus on the connector-specific logic. It provides:

- Base classes for implementing source and destination connectors
- Stream abstractions for handling data synchronization
- Built-in support for authentication, pagination, and error handling
- Declarative configuration for low-code connector development
- Utilities for testing and debugging connectors

## Quick Links

- [API Reference](api/index.md) - Detailed documentation of the CDK's components
  - [Core](api/core.md) - Core functionality and base classes
  - [Models](api/models.md) - Data models and protocol types
  - [Sources](api/sources.md) - Source connector components
  - [Destinations](api/destinations.md) - Destination connector components
- [Contributing Guide](CONTRIBUTING.md) - How to contribute to the CDK
- [GitHub Repository](https://github.com/airbytehq/airbyte-python-cdk)

## Getting Started

To create a new connector using the Python CDK:

1. Install the CDK:
```bash
pip install airbyte-cdk
```

2. Use the connector generator:
```bash
airbyte-cdk generate connector
```

For more detailed information about building connectors with the Python CDK, visit the [Airbyte Documentation](https://docs.airbyte.com/connector-development/).
