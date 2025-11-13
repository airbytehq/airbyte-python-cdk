# Airbyte Python CDK AI Development Guide

This guide provides essential context for AI agents working with the Airbyte Python CDK codebase.

## Project Overview

The Airbyte Python CDK is a framework for building Source Connectors for the Airbyte data integration platform. It provides components for:

- HTTP API connectors (REST, GraphQL)
- Declarative connectors using manifest files
- File-based source connectors
- Vector database destinations
- Concurrent data fetching

## Key Architectural Concepts

### Core Components

- **Source Classes**: Implement the `Source` interface in `airbyte_cdk.sources.source`. Base implementations include:
  - `AbstractSource` - Base class for Python sources
  - `DeclarativeSource` - For low-code connectors defined via manifest files
  - `ConcurrentSource` - For high-throughput parallel data fetching

- **Streams**: Core abstraction for data sources (`airbyte_cdk.sources.streams.Stream`). Key types:
  - `HttpStream` - Base class for HTTP API streams 
  - `DefaultStream` - Used with declarative sources
  - Concurrent streams in `airbyte_cdk.sources.streams.concurrent`

### Data Flow
1. Sources expose one or more Stream implementations
2. Streams define schema, state management, and record extraction
3. Records flow through the Airbyte protocol via standardized message types

## Development Conventions

### Testing Patterns

- Unit tests use pytest with scenarios pattern (`unit_tests/sources/**/test_*.py`)
- Mock HTTP responses with `HttpMocker` and response builders
- Standard test suite base classes in `airbyte_cdk.test.standard_tests`
- Use `@pytest.mark.parametrize` for test variations

### Source Implementation

- Prefer declarative manifests using `SourceDeclarativeManifest` for simple API connectors
- Extend base classes for custom logic:
  ```python
  from airbyte_cdk.sources import AbstractSource
  from airbyte_cdk.sources.streams import Stream
  
  class MySource(AbstractSource):
      def check_connection(...):
          # Verify credentials/connectivity
      
      def streams(self, config):
          return [MyStream(config)]
  ```

### State Management 

- Use `ConnectorStateManager` for handling incremental sync state
- Implement cursor fields in streams for incremental syncs
- State is persisted as JSON-serializable objects

## Common Workflows

### Building a New Connector

1. Start with [Connector Builder UI](https://docs.airbyte.com/connector-development/connector-builder-ui/overview)
2. For complex cases, use low-code CDK with manifest files
3. Custom Python implementation only when necessary

### Testing

```bash
pytest unit_tests/  # Run all tests
pytest unit_tests/sources/my_connector/  # Test specific connector
```

### Dependencies

- Manage with Poetry (`pyproject.toml`)
- Core requirements locked in `poetry.lock`
- Optional features via extras in `pyproject.toml`

## Integration Points

- Airbyte Protocol: Messages must conform to protocol models in `airbyte_cdk.models`
- External APIs: Use `HttpStream` with proper rate limiting
- Vector DBs: Implement destination logic using `destinations.vector_db_based`

## Key Files

- `airbyte_cdk/sources/abstract_source.py`: Base source implementation
- `airbyte_cdk/sources/streams/http/http.py`: HTTP stream base class
- `airbyte_cdk/sources/declarative/`: Low-code CDK components
- `unit_tests/sources/`: Test examples and patterns