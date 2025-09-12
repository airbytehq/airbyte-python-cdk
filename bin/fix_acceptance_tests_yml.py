#!/usr/bin/env python3
"""
Script to transform YAML acceptance tests structure.

This script transforms YAML files from the old format:
  tests:
    spec: [...]
    connection: [...]
    
To the new format:
  acceptance_tests:
    spec:
      tests: [...]
    connection:
      tests: [...]
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, Any


class FixingListIndentationDumper(yaml.Dumper):
    """
    The original indentation for list generates formatting issues on our side. So this dumper goes from:
    ```
    tests:
    - config_path: secrets/config.json
      status: succeed
    ```

    ...to:
    ```
    tests:
      - config_path: secrets/config.json
        status: succeed
    ```

    """
    def increase_indent(self, flow=False, indentless=False):
        return super(FixingListIndentationDumper, self).increase_indent(flow, False)


class AlreadyUpdatedError(Exception):
    """Exception raised when the YAML file has already been updated."""
    pass


def transform(file_path: Path) -> None:
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    
    if 'acceptance_tests' in data:
        raise AlreadyUpdatedError()

    if 'tests' not in data:
        raise ValueError(f"No 'tests' key found in {file_path}, skipping transformation")
    
    # Extract the tests data
    tests_data = data.pop('tests')
    
    if not isinstance(tests_data, dict):
        raise ValueError(f"Error: 'tests' key in {file_path} is not a dictionary")
    
    # Create the new acceptance_tests structure
    data['acceptance_tests'] = {}
    
    # Transform each test type
    for test_type, test_content in tests_data.items():
        data['acceptance_tests'][test_type] = {'tests': test_content}
    
    # Write back to file with preserved formatting
    with open(file_path, 'w') as f:
        yaml.dump(data, f, Dumper=FixingListIndentationDumper, default_flow_style=False, sort_keys=False)
    
    print(f"Successfully transformed {file_path}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_acceptance_tests_yml.py <airbyte_repo_path>")
        sys.exit(1)
    
    repo_path = Path(sys.argv[1])

    for file_path in repo_path.glob('airbyte-integrations/connectors/source-*/acceptance-test-config.yml'):
        try:
            transform(file_path)
        except AlreadyUpdatedError:
            print(f"File {file_path} has already been updated, skipping transformation")
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file {file_path}: {e}")
        except Exception as e:
            print(f"Error transforming {file_path}: {e}")


if __name__ == "__main__":
    main()
