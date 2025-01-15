"""Contains functions to compile custom code from text."""

import hashlib
import os
import sys
from types import ModuleType

from typing_extensions import Literal

ChecksumType = Literal["md5", "sha256"]
CHECKSUM_FUNCTIONS = {
    "md5": hashlib.md5,
    "sha256": hashlib.sha256,
}
COMPONENTS_MODULE_NAME = "components"
INJECTED_MANIFEST = "__injected_declarative_manifest"
INJECTED_COMPONENTS_PY = "__injected_components_py"
INJECTED_COMPONENTS_PY_CHECKSUMS = "__injected_components_py_checksums"
ENV_VAR_ALLOW_CUSTOM_CODE = "AIRBYTE_ALLOW_CUSTOM_CODE"


class AirbyteCodeTamperedError(Exception):
    """Raised when the connector's components module does not match its checksum.

    This is a fatal error, as it can be a sign of code tampering.
    """


class AirbyteCustomCodeNotPermittedError(Exception):
    """Raised when custom code is attempted to be run in an environment that does not support it."""

    def __init__(self) -> None:
        super().__init__(
            "Custom connector code is not permitted in this environment. "
            "If you need to run custom code, please ask your administrator to set the `AIRBYTE_ALLOW_CUSTOM_CODE` "
            "environment variable to 'true' in your Airbyte environment. "
            "If you see this message in Airbyte Cloud, your workspace does not allow executing "
            "custom connector code."
        )


def _hash_text(input_text: str, hash_type: Literal["md5", "sha256"] = "md5") -> str:
    """Return the hash of the input text using the specified hash type."""
    if not input_text:
        raise ValueError("Input text cannot be empty.")

    hash_object = CHECKSUM_FUNCTIONS[hash_type]()
    hash_object.update(input_text.encode())
    return hash_object.hexdigest()


def custom_code_execution_permitted() -> bool:
    """Return `True` if custom code execution is permitted, otherwise `False`.

    Custom code execution is permitted if the `AIRBYTE_ALLOW_CUSTOM_CODE` environment variable is set to 'true'.
    """
    return os.environ.get(ENV_VAR_ALLOW_CUSTOM_CODE, "").lower() == "true"


def validate_python_code(
    code_text: str,
    checksums: dict[ChecksumType, str] | None,
) -> None:
    """"""
    if not checksums:
        raise ValueError(f"A checksum is required to validate the code. Received: {checksums}")

    for checksum_type, checksum in checksums.items():
        if checksum_type not in CHECKSUM_FUNCTIONS:
            raise ValueError(
                f"Unsupported checksum type: {checksum_type}. Supported checksum types are: {CHECKSUM_FUNCTIONS.keys()}"
            )

        if checksum_type == "md5":
            if _hash_text(code_text, "md5") != checksum:
                raise AirbyteCodeTamperedError("MD5 checksum does not match.")
            continue

        if checksum_type == "sha256":
            if _hash_text(code_text, "sha256") != checksum:
                raise AirbyteCodeTamperedError("SHA256 checksum does not match.")
            continue


def components_module_from_string(components_py_text: str) -> ModuleType:
    """Load and return the components module from a provided string containing the python code.

    This assumes the components module is located at <connector_dir>/components.py.
    """
    module_name = "components"

    # Create a new module object
    components_module = ModuleType(name=module_name)

    # Execute the module text in the module's namespace
    exec(components_py_text, components_module.__dict__)

    # Add the module to sys.modules so it can be imported
    sys.modules[COMPONENTS_MODULE_NAME] = components_module

    # Now you can import and use the module
    return components_module
