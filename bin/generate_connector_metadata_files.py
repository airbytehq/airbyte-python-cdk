#!/usr/bin/env python3
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""
Generate Pydantic models and JSON schema for connector metadata validation.

This script downloads metadata schema YAML files from the airbyte monorepo and generates:
1. A single Python file with all Pydantic models (models.py)
2. A consolidated JSON schema file (metadata_schema.json)

The generated files are used for validating connector metadata.yaml files.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: pyyaml is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

OUTPUT_DIR_PATH = "airbyte_cdk/test/models/connector_metadata/generated"
AIRBYTE_REPO_URL = "https://github.com/airbytehq/airbyte.git"
SCHEMA_PATH = "airbyte-ci/connectors/metadata_service/lib/metadata_service/models/src"
DATAMODEL_CODEGEN_VERSION = "0.26.3"


def clone_schemas_from_github(temp_dir: Path) -> Path:
    """Clone metadata schema YAML files from GitHub using sparse checkout."""
    clone_dir = temp_dir / "airbyte"

    print("Cloning metadata schemas from airbyte repo...", file=sys.stderr)

    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--filter=blob:none",
            "--sparse",
            AIRBYTE_REPO_URL,
            str(clone_dir),
        ],
        check=True,
        capture_output=True,
    )

    subprocess.run(
        ["git", "-C", str(clone_dir), "sparse-checkout", "set", SCHEMA_PATH],
        check=True,
        capture_output=True,
    )

    schemas_dir = clone_dir / SCHEMA_PATH
    print(f"Cloned schemas to {schemas_dir}", file=sys.stderr)

    return schemas_dir


def generate_models_single_file(
    yaml_dir_path: Path,
    output_file_path: Path,
    temp_dir: Path,
) -> None:
    """Generate all metadata models into a single Python file using datamodel-codegen."""
    generated_temp = temp_dir / "generated_temp"
    generated_temp.mkdir(parents=True, exist_ok=True)

    print("Running datamodel-codegen via uvx...", file=sys.stderr)

    subprocess.run(
        [
            "uvx",
            "--from",
            f"datamodel-code-generator=={DATAMODEL_CODEGEN_VERSION}",
            "datamodel-codegen",
            "--input",
            str(yaml_dir_path),
            "--output",
            str(generated_temp),
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

    future_imports = set()
    stdlib_imports = set()
    third_party_imports = set()
    classes_and_updates = []

    for py_file in sorted(generated_temp.glob("*.py")):
        if py_file.name == "__init__.py":
            continue

        content = py_file.read_text()
        lines = content.split("\n")
        in_imports = True
        in_relative_import_block = False
        class_content = []

        for line in lines:
            if in_imports:
                if line.startswith("from __future__"):
                    future_imports.add(line)
                elif (
                    line.startswith("from datetime")
                    or line.startswith("from enum")
                    or line.startswith("from typing")
                    or line.startswith("from uuid")
                ):
                    stdlib_imports.add(line)
                elif line.startswith("from pydantic") or line.startswith("import "):
                    third_party_imports.add(line)
                elif line.startswith("from ."):
                    in_relative_import_block = True
                    if not line.rstrip().endswith(",") and not line.rstrip().endswith("("):
                        in_relative_import_block = False
                elif in_relative_import_block:
                    if line.strip().endswith(")"):
                        in_relative_import_block = False
                elif line.strip() and not line.startswith("#"):
                    in_imports = False
                    class_content.append(line)
            else:
                class_content.append(line)

        if class_content:
            classes_and_updates.append("\n".join(class_content))

    import_sections = []
    if future_imports:
        import_sections.append("\n".join(sorted(future_imports)))
    if stdlib_imports:
        import_sections.append("\n".join(sorted(stdlib_imports)))
    if third_party_imports:
        import_sections.append("\n".join(sorted(third_party_imports)))

    final_content = "\n\n".join(import_sections) + "\n\n\n" + "\n\n\n".join(classes_and_updates)

    post_processed_content = final_content.replace("from pydantic", "from pydantic.v1")

    lines = post_processed_content.split("\n")
    filtered_lines = []
    in_relative_import = False

    for line in lines:
        if line.strip().startswith("from . import"):
            in_relative_import = True
            if not line.rstrip().endswith(",") and not line.rstrip().endswith("("):
                in_relative_import = False
            continue

        if in_relative_import:
            if line.strip().endswith(")"):
                in_relative_import = False
            continue

        filtered_lines.append(line)

    post_processed_content = "\n".join(filtered_lines)

    output_file_path.write_text(post_processed_content)
    print(f"Generated models: {output_file_path}", file=sys.stderr)


def consolidate_yaml_schemas_to_json(yaml_dir_path: Path, output_json_path: Path) -> None:
    """Consolidate all YAML schemas into a single JSON schema file."""
    schemas = {}

    for yaml_file in yaml_dir_path.glob("*.yaml"):
        schema_name = yaml_file.stem
        schema_content = yaml.safe_load(yaml_file.read_text())
        schemas[schema_name] = schema_content

    all_schema_names = set(schemas.keys())

    for schema_content in schemas.values():
        if isinstance(schema_content, dict) and "definitions" in schema_content:
            all_schema_names.update(schema_content["definitions"].keys())

    def fix_refs(obj, in_definition=False):
        """Recursively fix $ref and type references in schema objects."""
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                if (key == "$id" or key == "$schema") and in_definition:
                    continue
                elif key == "$ref" and isinstance(value, str):
                    if value.endswith(".yaml"):
                        schema_name = value.replace(".yaml", "")
                        new_obj[key] = f"#/definitions/{schema_name}"
                    else:
                        new_obj[key] = value
                elif key == "type" and isinstance(value, str) and value in all_schema_names:
                    new_obj["$ref"] = f"#/definitions/{value}"
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
        # Create a consolidated schema with definitions
        consolidated = {
            "$schema": main_schema.get("$schema", "http://json-schema.org/draft-07/schema#"),
            "title": "Connector Metadata Schema",
            "description": "Consolidated JSON schema for Airbyte connector metadata validation",
            **main_schema,
            "definitions": {},
        }

        # Add all schemas (including their internal definitions) as top-level definitions
        for schema_name, schema_content in schemas.items():
            if schema_name != "ConnectorMetadataDefinitionV0":
                if isinstance(schema_content, dict) and "definitions" in schema_content:
                    for def_name, def_content in schema_content["definitions"].items():
                        consolidated["definitions"][def_name] = fix_refs(def_content, in_definition=True)
                    schema_without_defs = {k: v for k, v in schema_content.items() if k != "definitions"}
                    consolidated["definitions"][schema_name] = fix_refs(schema_without_defs, in_definition=True)
                else:
                    consolidated["definitions"][schema_name] = fix_refs(schema_content, in_definition=True)

        consolidated = fix_refs(consolidated, in_definition=False)

        output_json_path.write_text(json.dumps(consolidated, indent=2))
        print(f"Generated consolidated JSON schema: {output_json_path}", file=sys.stderr)
    else:
        print(
            "Warning: ConnectorMetadataDefinitionV0 not found, generating simple consolidation",
            file=sys.stderr,
        )
        output_json_path.write_text(json.dumps(schemas, indent=2))


def main():
    print("Generating connector metadata models...", file=sys.stderr)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        schemas_dir = clone_schemas_from_github(temp_path)

        output_dir = Path(OUTPUT_DIR_PATH)
        output_dir.mkdir(parents=True, exist_ok=True)

        print("Generating single Python file with all models...", file=sys.stderr)
        output_file = output_dir / "models.py"
        generate_models_single_file(
            yaml_dir_path=schemas_dir,
            output_file_path=output_file,
            temp_dir=temp_path,
        )

        print("Generating consolidated JSON schema...", file=sys.stderr)
        json_schema_file = output_dir / "metadata_schema.json"
        consolidate_yaml_schemas_to_json(schemas_dir, json_schema_file)

    print("Connector metadata model generation complete!", file=sys.stderr)


if __name__ == "__main__":
    main()
