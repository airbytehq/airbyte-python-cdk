# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte CDK CLI.

The Airbyte CDK provides command-line tools for working with connectors and related resources.


```bash
pip install airbyte-cdk

pipx run airbyte-cdk [command]
```



- `airbyte-cdk-build`: Build connector Docker images (legacy entry point)
- `source-declarative-manifest`: Run a declarative YAML manifest connector
- `airbyte-cdk`: Main CLI entry point with subcommands


The `airbyte-cdk` command includes subcommands organized by category:

```bash
airbyte-cdk image build [OPTIONS] CONNECTOR_DIR
```


For details on specific commands, see the documentation for each command module.
"""
