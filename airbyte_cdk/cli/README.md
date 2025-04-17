# Source Declarative Manifest CLI Usage Guide

This guide explains how to install and use the Source Declarative Manifest (SDM) CLI tool for Airbyte connector development.

## Installation

### Standard Installation

```bash
pipx install airbyte-cdk
```

### Alternative Installation (If encountering distutils errors)

If you encounter errors related to missing distutils module, try using this alternative approach:

```bash
python -m pip install --user pipx
python -m pipx install airbyte-cdk
```

## Using the CLI

The SDM CLI follows standard Airbyte connector command patterns:

```bash
source-declarative-manifest [command] --config /path/to/config.json
```

Where [command] can be:

spec - Show connector specification
check - Verify connection to the source
discover - List available streams
read - Read data from streams

:::caution
When developing locally (outside a Docker container), the CLI operates in "remote manifest mode" and expects your manifest to be included in your configuration file.
:::

### Steps for Local Testing

1. Convert your manifest from YAML to JSON

Your manifest is defined in YAML, but must be converted to JSON for the config file. You can use an [online tool](https://onlineyamltools.com/convert-yaml-to-json) to do so.

Create a config file that includes both your config parameters AND the manifest. Add your entire manifest as a JSON object under the `__injected_declarative_manifest` key

Example:

```json
{
  "api_key": "very_secret_key",
  "start_time": "04:20",
  "__injected_declarative_manifest": {
    // Insert the JSON version of your manifest here
  }
}
```

2. Run the command with your config file

```bash
source-declarative-manifest check --config /relative/path/to/your/config.json
```
