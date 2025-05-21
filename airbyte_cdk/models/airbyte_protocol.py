#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from functools import cached_property
from typing import Annotated, Any, Dict, List, Mapping, Optional, Union

import orjson
from airbyte_protocol_dataclasses.models import *  # noqa: F403  # Allow '*'
from serpyco_rs import CustomType, Serializer
from serpyco_rs.metadata import Alias

# ruff: noqa: F405  # ignore fuzzy import issues with 'import *'


@dataclass
class AirbyteStateBlob:
    """
    A dataclass that dynamically sets attributes based on provided keyword arguments and positional arguments.
    Used to "mimic" pydantic Basemodel with ConfigDict(extra='allow') option.

    The `AirbyteStateBlob` class allows for flexible instantiation by accepting any number of keyword arguments
    and positional arguments. These are used to dynamically update the instance's attributes. This class is useful
    in scenarios where the attributes of an object are not known until runtime and need to be set dynamically.

    Attributes:
        kwargs (InitVar[Mapping[str, Any]]): A dictionary of keyword arguments used to set attributes dynamically.

    Methods:
        __init__(*args: Any, **kwargs: Any) -> None:
            Initializes the `AirbyteStateBlob` by setting attributes from the provided arguments.

        __eq__(other: object) -> bool:
            Checks equality between two `AirbyteStateBlob` instances based on their internal dictionaries.
            Returns `False` if the other object is not an instance of `AirbyteStateBlob`.
    """

    kwargs: InitVar[Mapping[str, Any]]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Set any attribute passed in through kwargs
        for arg in args:
            self.__dict__.update(arg)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __eq__(self, other: object) -> bool:
        return (
            False
            if not isinstance(other, AirbyteStateBlob)
            else bool(self.__dict__ == other.__dict__)
        )


class AirbyteStateBlobType(CustomType[AirbyteStateBlob, dict[str, Any]]):
    def serialize(self, value: AirbyteStateBlob) -> dict[str, Any]:
        # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
        return {k: v for k, v in value.__dict__.items()}

    def deserialize(self, value: dict[str, Any]) -> AirbyteStateBlob:
        return AirbyteStateBlob(value)

    def get_json_schema(self) -> dict[str, Any]:
        return {"type": "object"}


def custom_type_resolver(t: type) -> CustomType[AirbyteStateBlob, dict[str, Any]] | None:
    return AirbyteStateBlobType() if t is AirbyteStateBlob else None


# The following dataclasses have been redeclared to include the new version of AirbyteStateBlob
@dataclass
class AirbyteStreamState:
    stream_descriptor: StreamDescriptor  # type: ignore [name-defined]
    stream_state: Optional[AirbyteStateBlob] = None


@dataclass
class AirbyteGlobalState:
    stream_states: List[AirbyteStreamState]
    shared_state: Optional[AirbyteStateBlob] = None


@dataclass
class AirbyteStateMessage:
    type: Optional[AirbyteStateType] = None  # type: ignore [name-defined]
    stream: Optional[AirbyteStreamState] = None
    global_: Annotated[AirbyteGlobalState | None, Alias("global")] = (
        None  # "global" is a reserved keyword in python ⇒ Alias is used for (de-)serialization
    )
    data: Optional[Dict[str, Any]] = None
    sourceStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]
    destinationStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]

    def to_dict(self) -> dict:
        return self._serializer.dump(self)

    def to_string(self) -> str:
        return orjson.dumps(self.to_dict()).decode("utf-8")

    def from_string(self, string: str, /) -> "AirbyteMessage":
        """Deserialize a string into an AirbyteMessage object."""
        return self._serializer.load(orjson.loads(string))

    def from_dict(self, dictionary: dict, /) -> "AirbyteMessage":
        """Deserialize a dictionary into an AirbyteMessage object."""
        return self._serializer.load(dictionary)

    @cached_property
    @classmethod
    def _serializer(cls) -> Serializer:
        """
        Returns a serializer for the AirbyteMessage class.
        The serializer is cached for performance reasons.
        """
        return Serializer(
            AirbyteStateMessage,
            omit_none=True,
            custom_type_resolver=custom_type_resolver,
        )


@dataclass
class AirbyteMessage:
    type: Type  # type: ignore [name-defined]
    log: Optional[AirbyteLogMessage] = None  # type: ignore [name-defined]
    spec: Optional[ConnectorSpecification] = None  # type: ignore [name-defined]
    connectionStatus: Optional[AirbyteConnectionStatus] = None  # type: ignore [name-defined]
    catalog: Optional[AirbyteCatalog] = None  # type: ignore [name-defined]
    record: Optional[AirbyteRecordMessage] = None  # type: ignore [name-defined]
    state: Optional[AirbyteStateMessage] = None
    trace: Optional[AirbyteTraceMessage] = None  # type: ignore [name-defined]
    control: Optional[AirbyteControlMessage] = None  # type: ignore [name-defined]

    def to_dict(self) -> dict:
        return self._serializer.dump(self)

    def to_string(self) -> str:
        return orjson.dumps(self.to_dict()).decode("utf-8")

    def from_string(self, string: str, /) -> "AirbyteMessage":
        """Deserialize a string into an AirbyteMessage object."""
        return self._serializer.load(orjson.loads(string))

    def from_dict(self, dictionary: dict, /) -> "AirbyteMessage":
        """Deserialize a dictionary into an AirbyteMessage object."""
        return self._serializer.load(dictionary)

    @cached_property
    @classmethod
    def _serializer(cls) -> Serializer:
        """
        Returns a serializer for the AirbyteMessage class.
        The serializer is cached for performance reasons.
        """
        return Serializer(
            AirbyteMessage,
            omit_none=True,
            custom_type_resolver=custom_type_resolver,
        )
