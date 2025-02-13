# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""CLI for running test reads from the Airbyte Connector Builder.

This CLI accepts a config file and an action to perform on the connector.

Usage:
    connector-builder-test-read [--config CONFIG] [--action ACTION]

Options:
    --action ACTION    The action to perform (e.g., test, validate).
    --config CONFIG    Path to the config file.
"""
from airbyte_cdk.cli.connector_builder_test_read.run import run

__all__ = [
    "run",
]
