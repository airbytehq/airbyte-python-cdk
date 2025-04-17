# Airbyte CDK Build Command

The `airbyte-cdk-build` command provides a simplified way to build connector Docker images without requiring the full Airbyte CI pipeline.

## Installation

```bash
# Install the CDK with pip
pip install airbyte-cdk

# Or using pipx for direct execution
pipx run airbyte-cdk-build [arguments]
```

## Usage

```bash
# Legacy entry point (still supported)
airbyte-cdk-build /path/to/connector

# New CLI interface
airbyte-cdk image build /path/to/connector

# With custom tag
airbyte-cdk image build /path/to/connector --tag custom_tag

# Build for a specific platform
airbyte-cdk image build /path/to/connector --platform linux/arm64

# Skip verification
airbyte-cdk image build /path/to/connector --no-verify

# Enable verbose logging
airbyte-cdk image build /path/to/connector --verbose
```

## Options

- `connector_dir`: Path to the connector directory (required)
- `--tag`: Tag to apply to the built image (default: "dev")
- `--platform`: Platform to build for (choices: "linux/amd64", "linux/arm64", default: "linux/amd64")
- `--no-verify`: Skip verification of the built image
- `--verbose`, `-v`: Enable verbose logging

## How It Works

The command reads the connector's metadata from the `metadata.yaml` file, builds a Docker image using the connector's Dockerfile, and verifies the image by running the `spec` command. The image is tagged according to the repository name specified in the metadata and the provided tag.

This command is designed to be a simpler alternative to the `airbyte-ci build` command, using Docker directly on the host machine instead of Dagger.
