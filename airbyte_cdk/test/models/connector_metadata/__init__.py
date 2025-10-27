"""Pydantic and JSON Schema models for `metadata.yaml` validation and testing.

## Usage

```python
from airbyte_cdk.test.models import ConnectorMetadataDefinitionV0
import yaml

metadata = ConnectorMetadataDefinitionV0(**yaml.safe_load(metadata_yaml))
```

## Regenerating Models

These models are auto-generated from JSON schemas in the airbytehq/airbyte repository.
For information on regenerating these models, see the Contributing Guide:
https://github.com/airbytehq/airbyte-python-cdk/blob/main/docs/CONTRIBUTING.md#regenerating-connector-metadata-models
"""

from .generated.models import ConnectorMetadataDefinitionV0, ConnectorTestSuiteOptions

__all__ = [
    "ConnectorMetadataDefinitionV0",
    "ConnectorTestSuiteOptions",
]
