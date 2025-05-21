# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""CLI Argument Parsing Utilities."""

import argparse
import json
from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _read_json_file(
    file_path: Path | str,
) -> dict[str, Any]:
    """Read a JSON file and return its contents as a dictionary.

    Raises ValueError if the file cannot be read or is not valid JSON.
    """
    file_text = Path(file_path).read_text()

    try:
        return json.loads(file_text)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Could not read json file {file_path}: {error}. Please ensure that it is a valid JSON."
        )


@dataclass(kw_only=True)
class ConnectorCLIArgs:
    command: str
    debug: bool
    config: str | None = None
    state: str | None = None
    catalog: str | None = None
    manifest_path: str | None = None
    components_path: str | None = None

    def get_config_dict(
        self,
        *,
        allow_missing: bool = False,
    ) -> MutableMapping[str, Any]:
        """Read the config file and return its contents as a dictionary.

        If allow_missing is True, return an empty dictionary when the config file is not provided.
        """
        if self.config is None:
            if not allow_missing:
                raise ValueError("Config file path is required.")

            return {}

        config = _read_json_file(self.config)
        if isinstance(config, MutableMapping):
            return config
        else:
            raise ValueError(
                f"The content of {self.config} is not an object and therefore is not a valid config. Please ensure the file represent a config."
            )


def parse_cli_args(
    args: list[str],
    *,
    with_read: bool = True,
    with_discover: bool = True,
    with_write: bool = False,
) -> ConnectorCLIArgs:
    """Return the parsed CLI arguments for the connector.

    The caller can validate the arguments and use them as needed. This function allows all possible
    arguments to be passed in, but the caller should only use the ones that are relevant to the
    command being executed. By default, we expect a typical "source" configuration.

    Optionally, caller may specify command availability by overriding the `with` flags.
    """
    # set up parent parsers
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--debug", action="store_true", help="enables detailed debug logs related to the sync"
    )
    main_parser = argparse.ArgumentParser()
    subparsers = main_parser.add_subparsers(
        title="commands",
        dest="command",
        required=True,
    )

    # spec
    subparsers.add_parser(
        "spec", help="outputs the json configuration specification", parents=[parent_parser]
    )

    # check
    check_parser = subparsers.add_parser(
        "check", help="checks the config can be used to connect", parents=[parent_parser]
    )
    required_check_parser = check_parser.add_argument_group("required named arguments")
    required_check_parser.add_argument(
        "--config", type=str, required=True, help="path to the json configuration file"
    )
    check_parser.add_argument(
        "--manifest-path",
        type=str,
        required=False,
        help="path to the YAML manifest file to inject into the config",
    )
    check_parser.add_argument(
        "--components-path",
        type=str,
        required=False,
        help="path to the custom components file, if it exists",
    )

    if with_discover:
        discover_parser = subparsers.add_parser(
            "discover",
            help="outputs a catalog describing the source's schema",
            parents=[parent_parser],
        )
        required_discover_parser = discover_parser.add_argument_group("required named arguments")
        required_discover_parser.add_argument(
            "--config", type=str, required=True, help="path to the json configuration file"
        )
        discover_parser.add_argument(
            "--manifest-path",
            type=str,
            required=False,
            help="path to the YAML manifest file to inject into the config",
        )
        discover_parser.add_argument(
            "--components-path",
            type=str,
            required=False,
            help="path to the custom components file, if it exists",
        )

    if with_read:
        read_parser = subparsers.add_parser(
            "read", help="reads the source and outputs messages to STDOUT", parents=[parent_parser]
        )

        read_parser.add_argument(
            "--state", type=str, required=False, help="path to the json-encoded state file"
        )
        required_read_parser = read_parser.add_argument_group("required named arguments")
        required_read_parser.add_argument(
            "--config", type=str, required=True, help="path to the json configuration file"
        )
        required_read_parser.add_argument(
            "--catalog",
            type=str,
            required=True,
            help="path to the catalog used to determine which data to read",
        )
        read_parser.add_argument(
            "--manifest-path",
            type=str,
            required=False,
            help="path to the YAML manifest file to inject into the config",
        )
        read_parser.add_argument(
            "--components-path",
            type=str,
            required=False,
            help="path to the custom components file, if it exists",
        )

    if with_write:
        # write
        write_parser = subparsers.add_parser(
            "write", help="Writes data to the destination", parents=[parent_parser]
        )
        write_required = write_parser.add_argument_group("required named arguments")
        write_required.add_argument(
            "--config", type=str, required=True, help="path to the JSON configuration file"
        )
        write_required.add_argument(
            "--catalog", type=str, required=True, help="path to the configured catalog JSON file"
        )

    parsed_args: argparse.Namespace = main_parser.parse_args(args)
    return ConnectorCLIArgs(
        command=parsed_args.command,
        debug=parsed_args.debug,
        config=parsed_args.config,
        state=parsed_args.state,
        catalog=parsed_args.catalog,
        manifest_path=parsed_args.manifest_path,
        components_path=parsed_args.components_path,
    )
