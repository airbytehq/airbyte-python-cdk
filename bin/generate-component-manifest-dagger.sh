#!/usr/bin/env bash

# Generate component manifest files using datamodel-codegen.

set -e

uv run bin/generate_component_manifest_files.py
