# Airbyte CDK CLI

The Airbyte CDK provides command-line tools for working with connectors and related resources.

## Installation

```bash
# Install the CDK with pip
pip install airbyte-cdk

# Or using pipx for direct execution
pipx run airbyte-cdk [command]
```

## Available Commands

### Top-Level Commands

- `airbyte-cdk-build`: Build connector Docker images (legacy entry point)
- `source-declarative-manifest`: Run a declarative YAML manifest connector
- `airbyte-cdk`: Main CLI entry point with subcommands

### Subcommands

The `airbyte-cdk` command includes subcommands organized by category:

```bash
# Image-related commands
airbyte-cdk image build [OPTIONS] CONNECTOR_DIR
```

## Command Documentation

For details on specific commands, see:

- [Build Command](./build.md): Build connector Docker images
