# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import json
import logging
import sys
from enum import Enum
from typing import Any, Callable, Dict, Type, TypeVar, cast

import dacite
import orjson
from pydantic import ValidationError

from .airbyte_protocol import (  # type: ignore[attr-defined] # all classes are imported to airbyte_protocol via *
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
)

USE_RUST_BACKEND = sys.platform != "emscripten"
"""When run in WASM, use the pure Python backend for serpyco."""

_HAS_LOGGED_FOR_SERIALIZATION_ERROR = False
"""Track if we have logged an error for serialization issues."""

T = TypeVar("T")

logger = logging.getLogger("airbyte")

# Making this a no-op for now:


def ab_message_to_string(
    message: AirbyteMessage,
) -> str:
    """
    Convert an AirbyteMessage to a JSON string.

    Args:
        message (AirbyteMessage): The Airbyte message to convert.

    Returns:
        str: JSON string representation of the AirbyteMessage.
    """
    return message.model_dump_json()


def ab_message_from_string(
    message_json: str,
) -> AirbyteMessage:
    """
    Convert a JSON string to an AirbyteMessage.

    Args:
        message_str (str): The JSON string to convert.

    Returns:
        AirbyteMessage: The deserialized AirbyteMessage.
    """
    try:
        return AirbyteMessage.model_validate_json(message_json)
    except ValidationError as e:
        raise ValueError(f"Invalid AirbyteMessage format: {e}") from e
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON: {e}") from e


def ab_connector_spec_from_string(
    spec_json: str,
) -> ConnectorSpecification:
    """
    Convert a JSON string to a ConnectorSpecification.

    Args:
        spec_str (str): The JSON string to convert.

    Returns:
        ConnectorSpecification: The deserialized ConnectorSpecification.
    """
    try:
        return ConnectorSpecification.model_validate_json(spec_json)
    except ValidationError as e:
        raise ValueError(f"Invalid ConnectorSpecification format: {e}") from e
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON: {e}") from e


def ab_connector_spec_to_string(
    spec: ConnectorSpecification,
) -> str:
    """
    Convert a ConnectorSpecification to a JSON string.

    Args:
        spec (ConnectorSpecification): The ConnectorSpecification to convert.

    Returns:
        str: JSON string representation of the ConnectorSpecification.
    """
    return spec.model_dump_json()


def ab_configured_catalog_to_string(
    catalog: ConfiguredAirbyteCatalog,
) -> str:
    """
    Convert a ConfiguredAirbyteCatalog to a JSON string.

    Args:
        catalog (ConfiguredAirbyteCatalog): The ConfiguredAirbyteCatalog to convert.

    Returns:
        str: JSON string representation of the ConfiguredAirbyteCatalog.
    """
    return catalog.model_dump_json()


def ab_configured_catalog_from_string(
    catalog_json: str,
) -> ConfiguredAirbyteCatalog:
    """
    Convert a JSON string to a ConfiguredAirbyteCatalog.

    Args:
        catalog_json (str): The JSON string to convert.

    Returns:
        ConfiguredAirbyteCatalog: The deserialized ConfiguredAirbyteCatalog.
    """
    try:
        return ConfiguredAirbyteCatalog.model_validate_json(catalog_json)
    except ValidationError as e:
        raise ValueError(f"Invalid ConfiguredAirbyteCatalog format: {e}") from e
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON: {e}") from e


def ab_state_message_from_string(
    state_json: str,
) -> AirbyteStateMessage:
    """
    Convert a JSON string to an AirbyteStateMessage.

    Args:
        state_json (str): The JSON string to convert.

    Returns:
        AirbyteStateMessage: The deserialized AirbyteStateMessage.
    """
    try:
        return AirbyteStateMessage.model_validate_json(state_json)
    except ValidationError as e:
        raise ValueError(f"Invalid AirbyteStateMessage format: {e}") from e
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON: {e}") from e


def ab_state_message_to_string(
    state: AirbyteStateMessage,
) -> str:
    """
    Convert an AirbyteStateMessage to a JSON string.

    Args:
        state (AirbyteStateMessage): The AirbyteStateMessage to convert.

    Returns:
        str: JSON string representation of the AirbyteStateMessage.
    """
    return state.model_dump_json()
