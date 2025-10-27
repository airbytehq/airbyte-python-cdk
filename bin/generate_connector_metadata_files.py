#!/usr/bin/env python3
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""
Generate Pydantic models and JSON schema for connector metadata validation.

This script uses vendored metadata schema YAML files and generates:
1. A consolidated JSON schema file (metadata_schema.json)
2. A single Python file with all Pydantic models (models.py) generated from the JSON schema

The generated files are used for validating connector metadata.yaml files.
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("Error: pyyaml is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

OUTPUT_DIR_PATH = "airbyte_cdk/test/models/connector_metadata/generated"
VENDORED_SCHEMAS_PATH = "airbyte_cdk/models/connector_metadata/resources"
DATAMODEL_CODEGEN_VERSION = "0.26.3"


def get_vendored_schemas_dir() -> Path:
    """Get the path to vendored metadata schema YAML files."""
    schemas_dir = Path(__file__).parent.parent / VENDORED_SCHEMAS_PATH

    if not schemas_dir.exists():
        print(f"Error: Vendored schemas directory not found: {schemas_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Using vendored schemas from {schemas_dir}", file=sys.stderr)
    return schemas_dir


def consolidate_yaml_schemas_to_json(yaml_dir_path: Path, output_json_path: Path) -> None:
    """Consolidate all YAML schemas into a single JSON schema file."""
    schemas = {}

    for yaml_file in yaml_dir_path.glob("*.yaml"):
        schema_name = yaml_file.stem
        schema_content = yaml.safe_load(yaml_file.read_text())
        schemas[schema_name] = schema_content

    all_schema_names = set(schemas.keys())
    json_primitives = {"string", "number", "integer", "boolean", "object", "array", "null"}

    for schema_content in schemas.values():
        if isinstance(schema_content, dict) and "definitions" in schema_content:
            all_schema_names.update(schema_content["definitions"].keys())

    def fix_refs(obj: Any, in_definition: bool = False) -> Any:
        """Recursively fix $ref and type references in schema objects."""
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                if (key == "$id" or key == "$schema") and in_definition:
                    continue
                elif key == "$ref" and isinstance(value, str):
                    m = re.match(r"(?:.*/)?(?P<name>[^/#]+)\.yaml(?P<frag>#.*)?$", value)
                    if m:
                        schema_name = m.group("name")
                        frag = m.group("frag") or ""
                        new_obj[key] = f"#/definitions/{schema_name}{frag}"
                    else:
                        new_obj[key] = value
                elif key == "type" and isinstance(value, str):
                    if value in all_schema_names and value not in json_primitives:
                        new_obj["$ref"] = f"#/definitions/{value}"
                    else:
                        new_obj[key] = value
                elif key == "type" and value == "const":
                    pass
                else:
                    new_obj[key] = fix_refs(value, in_definition=in_definition)
            return new_obj
        elif isinstance(obj, list):
            return [fix_refs(item, in_definition=in_definition) for item in obj]
        else:
            return obj

    # Find the main schema (ConnectorMetadataDefinitionV0)
    main_schema = schemas.get("ConnectorMetadataDefinitionV0")

    if main_schema:
        # Create a consolidated schema preserving main schema structure
        consolidated = dict(main_schema)  # shallow copy
        consolidated.setdefault("$schema", "http://json-schema.org/draft-07/schema#")
        consolidated.setdefault("title", "Connector Metadata Schema")
        consolidated.setdefault(
            "description", "Consolidated JSON schema for Airbyte connector metadata validation"
        )

        consolidated_definitions = dict(consolidated.get("definitions", {}))

        # Add all schemas (including their internal definitions) as top-level definitions
        for schema_name, schema_content in schemas.items():
            if schema_name != "ConnectorMetadataDefinitionV0":
                if isinstance(schema_content, dict) and "definitions" in schema_content:
                    for def_name, def_content in schema_content["definitions"].items():
                        consolidated_definitions[def_name] = fix_refs(
                            def_content, in_definition=True
                        )
                    schema_without_defs = {
                        k: v for k, v in schema_content.items() if k != "definitions"
                    }
                    consolidated_definitions[schema_name] = fix_refs(
                        schema_without_defs, in_definition=True
                    )
                else:
                    consolidated_definitions[schema_name] = fix_refs(
                        schema_content, in_definition=True
                    )

        consolidated["definitions"] = consolidated_definitions
        consolidated = fix_refs(consolidated, in_definition=False)

        output_json_path.write_text(json.dumps(consolidated, indent=2))
        print(f"Generated consolidated JSON schema: {output_json_path}", file=sys.stderr)
    else:
        print(
            "Warning: ConnectorMetadataDefinitionV0 not found, generating simple consolidation",
            file=sys.stderr,
        )
        output_json_path.write_text(json.dumps(schemas, indent=2))


def generate_models_from_json_schema(json_schema_path: Path, output_file_path: Path) -> None:
    """Generate Pydantic models from consolidated JSON schema."""
    print("Running datamodel-codegen via uvx...", file=sys.stderr)

    subprocess.run(
        [
            "uvx",
            "--from",
            f"datamodel-code-generator=={DATAMODEL_CODEGEN_VERSION}",
            "datamodel-codegen",
            "--input",
            str(json_schema_path),
            "--output",
            str(output_file_path),
            "--input-file-type",
            "jsonschema",
            "--disable-timestamp",
            "--enum-field-as-literal",
            "one",
            "--set-default-enum-member",
            "--use-double-quotes",
            "--remove-special-field-name-prefix",
            "--field-extra-keys",
            "deprecated",
            "deprecation_message",
        ],
        check=True,
    )

    content = output_file_path.read_text()
    content = content.replace("from pydantic", "from pydantic.v1")
    output_file_path.write_text(content)

    print(f"Generated models: {output_file_path}", file=sys.stderr)


def main() -> None:
    print("Generating connector metadata models...", file=sys.stderr)

    schemas_dir = get_vendored_schemas_dir()

    output_dir = Path(OUTPUT_DIR_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Consolidating YAML schemas into JSON...", file=sys.stderr)
    json_schema_file = output_dir / "metadata_schema.json"
    consolidate_yaml_schemas_to_json(schemas_dir, json_schema_file)

    print("Generating Python models from JSON schema...", file=sys.stderr)
    output_file = output_dir / "models.py"
    generate_models_from_json_schema(json_schema_file, output_file)

    print("Connector metadata model generation complete!", file=sys.stderr)


if __name__ == "__main__":
    main()
