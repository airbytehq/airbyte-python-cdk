# Airbyte Python CDK and Low-Code CDK

Airbyte Python CDK is a framework for building Airbyte API Source Connectors. It provides a set of
classes and helpers that make it easy to build a connector against an HTTP API (REST, GraphQL, etc),
or a generic Python source connector.

## Building Connectors with the CDK

If you're looking to build a connector, we highly recommend that you first
[start with the Connector Builder](https://docs.airbyte.com/connector-development/connector-builder-ui/overview).
It should be enough for 90% connectors out there. For more flexible and complex connectors, use the
[low-code CDK and `SourceDeclarativeManifest`](https://docs.airbyte.com/connector-development/config-based/low-code-cdk-overview).

For more information on building connectors, please see the [Connector Development](https://docs.airbyte.com/connector-development/) guide on [docs.airbyte.com](https://docs.airbyte.com).

## Python CDK Overview

Airbyte CDK code is within `airbyte_cdk` directory. Here's a high level overview of what's inside:

- `airbyte_cdk/connector_builder`. Internal wrapper that helps the Connector Builder platform run a declarative manifest (low-code connector). You should not use this code directly. If you need to run a `SourceDeclarativeManifest`, take a look at [`source-declarative-manifest`](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-declarative-manifest) connector implementation instead.
- `airbyte_cdk/cli/source_declarative_manifest`. This module defines the `source-declarative-manifest` (aka "SDM") connector execution logic and associated CLI.
- `airbyte_cdk/destinations`. Basic Destination connector support! If you're building a Destination connector in Python, try that. Some of our vector DB destinations like `destination-pinecone` are using that code.
- `airbyte_cdk/models` expose `airbyte_protocol.models` as a part of `airbyte_cdk` package.
- `airbyte_cdk/sources/concurrent_source` is the Concurrent CDK implementation. It supports reading data from streams concurrently per slice / partition, useful for connectors with high throughput and high number of records.
- `airbyte_cdk/sources/declarative` is the low-code CDK. It works on top of Airbyte Python CDK, but provides a declarative manifest language to define streams, operations, etc. This makes it easier to build connectors without writing Python code.
- `airbyte_cdk/sources/file_based` is the CDK for file-based sources. Examples include S3, Azure, GCS, etc.

## Contributing

For instructions on how to contribute, please see our [Contributing Guide](docs/CONTRIBUTING.md).

## Release Management

Please see the [Release Management](docs/RELEASES.md) guide for information on how to perform releases and pre-releases.
