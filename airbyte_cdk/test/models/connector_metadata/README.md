# Airbyte Connector Metadata Models

This package contains Pydantic models for validating Airbyte connector `metadata.yaml` files.

## Overview

The models are automatically generated from JSON Schema YAML files maintained in the [airbytehq/airbyte](https://github.com/airbytehq/airbyte) repository at:
```
airbyte-ci/connectors/metadata_service/lib/metadata_service/models/src/
```

During the CDK build process (`poetry run poe build`), these schemas are downloaded from GitHub and used to generate Pydantic models via `datamodel-code-generator`.

## Usage

### Validating a metadata.yaml file

```python
from pathlib import Path
import yaml
from airbyte_cdk.test.models.connector_metadata import ConnectorMetadataDefinitionV0

# Load metadata.yaml
metadata_path = Path("path/to/metadata.yaml")
metadata_dict = yaml.safe_load(metadata_path.read_text())

# Validate using Pydantic
try:
    metadata = ConnectorMetadataDefinitionV0(**metadata_dict)
    print("✓ Metadata is valid!")
except Exception as e:
    print(f"✗ Validation failed: {e}")
```

### Accessing metadata fields

```python
from airbyte_cdk.test.models.connector_metadata import ConnectorMetadataDefinitionV0

metadata = ConnectorMetadataDefinitionV0(**metadata_dict)

# Access fields with full type safety
print(f"Connector: {metadata.data.name}")
print(f"Docker repository: {metadata.data.dockerRepository}")
print(f"Docker image tag: {metadata.data.dockerImageTag}")
print(f"Support level: {metadata.data.supportLevel}")
```

### Available models

The main model is `ConnectorMetadataDefinitionV0`, which includes nested models for:

- `ConnectorType` - Source or destination
- `ConnectorSubtype` - API, database, file, etc.
- `SupportLevel` - Community, certified, etc.
- `ReleaseStage` - Alpha, beta, generally_available
- `ConnectorBreakingChanges` - Breaking change definitions
- `ConnectorReleases` - Release information
- `AllowedHosts` - Network access configuration
- And many more...

## Regenerating Models

Models are regenerated automatically when you run:

```bash
poetry run poe build
```

This command:
1. Downloads the latest schema YAML files from the airbyte repository
2. Generates Pydantic models using `datamodel-code-generator`
3. Outputs models to `airbyte_cdk/test/models/connector_metadata/`

## Schema Source

The authoritative schemas are maintained in the [airbyte monorepo](https://github.com/airbytehq/airbyte/tree/master/airbyte-ci/connectors/metadata_service/lib/metadata_service/models/src).

Any changes to metadata validation should be made there, and will be automatically picked up by the CDK build process.
