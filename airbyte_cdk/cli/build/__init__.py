"""Airbyte CDK Build Command.

The `airbyte-cdk-build` command provides a simplified way to build connector Docker images without requiring the full Airbyte CI pipeline.


```bash
pip install airbyte-cdk

pipx run airbyte-cdk-build [arguments]
```


```bash
airbyte-cdk-build /path/to/connector

airbyte-cdk image build /path/to/connector

airbyte-cdk image build /path/to/connector --tag custom_tag

airbyte-cdk image build /path/to/connector --no-verify

airbyte-cdk image build /path/to/connector --verbose
```


- `connector_dir`: Path to the connector directory (required)
- `--tag`: Tag to apply to the built image (default: "dev")
- `--no-verify`: Skip verification of the built image
- `--verbose`, `-v`: Enable verbose logging


The command reads the connector's metadata from the `metadata.yaml` file, builds a Docker image using the connector's Dockerfile, and verifies the image by running the `spec` command. The image is tagged according to the repository name specified in the metadata and the provided tag.

This command is designed to be a simpler alternative to the `airbyte-ci build` command, using Docker directly on the host machine instead of Dagger.
"""

from airbyte_cdk.cli.build._run import run

__all__ = [
    "run",
]
