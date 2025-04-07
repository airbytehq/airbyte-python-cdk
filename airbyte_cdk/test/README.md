# Airbyte CDK Test Extras

This module provides test utilities and fixtures for Airbyte connectors, including pre-built test suites that connector developers can easily use to run a full suite of tests.

## Usage

### Option 1: Using the built-in pytest plugin (recommended)

The CDK includes a pytest plugin that automatically discovers connectors and their tests, eliminating the need for scaffolding files. To use it:

1. Install the Airbyte CDK with test extras:

```bash
poetry add airbyte-cdk[tests]
```

Or if you're developing the CDK itself:

```bash
poetry install --extras tests
```

2. Run pytest in your connector directory with auto-discovery enabled:

```bash
pytest --auto-discover
```

If your connector is in a different directory, you can specify it:

```bash
pytest --auto-discover --connector-dir /path/to/connector
```

The plugin will:
- Automatically discover your connector type (source or destination)
- Find your connector class
- Load test scenarios from your acceptance test config file
- Run the appropriate tests

### Option 2: Creating a minimal test scaffold (traditional approach)

If you prefer more control over test discovery and execution, you can create a minimal test scaffold:

1. Create a test file (e.g., `test_connector.py`):

```python
from airbyte_cdk.test.declarative.test_suites.source_base import SourceTestSuiteBase
from your_connector.source import YourConnector

class TestYourConnector(SourceTestSuiteBase):
    connector = YourConnector
    
    @classmethod
    def create_connector(cls, scenario):
        return cls.connector()
```

2. Run pytest with the connector option:

```bash
pytest --run-connector
```

## Acceptance Test Config

The test suites will automatically look for and use an acceptance test config file named either:
- `connector-acceptance-tests.yml`
- `acceptance-test-config.yml`

The config file is used to:
- Discover test scenarios
- Configure test behavior
- Set expectations for test results

## Available Test Suites

- `ConnectorTestSuiteBase`: Base test suite for all connectors
- `SourceTestSuiteBase`: Test suite for source connectors
- `DestinationTestSuiteBase`: Test suite for destination connectors
