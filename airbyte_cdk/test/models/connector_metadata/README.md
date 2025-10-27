# Airbyte Connector Metadata Models

This package contains Pydantic models for validating Airbyte connector `metadata.yaml` files.

## Usage

```python
from airbyte_cdk.test.models import ConnectorMetadataDefinitionV0
import yaml

metadata = ConnectorMetadataDefinitionV0(**yaml.safe_load(metadata_yaml))
```

## Regenerating Models

See the [Contributing Guide](../../../docs/CONTRIBUTING.md#regenerating-connector-metadata-models) for information on regenerating these models.
