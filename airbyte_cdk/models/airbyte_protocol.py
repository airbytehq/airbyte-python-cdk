#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from typing import Annotated, Any

from airbyte_protocol_dataclasses.models import *  # noqa: F403  # Allow '*'
from serpyco_rs.metadata import Alias

from airbyte_cdk.models.file_transfer_record_message import AirbyteFileTransferRecordMessage

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


# The following dataclasses have been redeclared to include the new version of AirbyteStateBlob
@dataclass
class AirbyteStreamState:
    stream_descriptor: StreamDescriptor  # type: ignore [name-defined]
    stream_state: AirbyteStateBlob | None = None


@dataclass
class AirbyteGlobalState:
    stream_states: list[AirbyteStreamState]
    shared_state: AirbyteStateBlob | None = None


@dataclass
class AirbyteStateMessage:
    type: AirbyteStateType | None = None  # type: ignore [name-defined]
    stream: AirbyteStreamState | None = None
    global_: Annotated[AirbyteGlobalState | None, Alias("global")] = (
        None  # "global" is a reserved keyword in python ⇒ Alias is used for (de-)serialization
    )
    data: dict[str, Any] | None = None
    sourceStats: AirbyteStateStats | None = None  # type: ignore [name-defined]
    destinationStats: AirbyteStateStats | None = None  # type: ignore [name-defined]


@dataclass
class AirbyteMessage:
    type: Type  # type: ignore [name-defined]
    log: AirbyteLogMessage | None = None  # type: ignore [name-defined]
    spec: ConnectorSpecification | None = None  # type: ignore [name-defined]
    connectionStatus: AirbyteConnectionStatus | None = None  # type: ignore [name-defined]
    catalog: AirbyteCatalog | None = None  # type: ignore [name-defined]
    record: AirbyteFileTransferRecordMessage | AirbyteRecordMessage | None = None  # type: ignore [name-defined]
    state: AirbyteStateMessage | None = None
    trace: AirbyteTraceMessage | None = None  # type: ignore [name-defined]
    control: AirbyteControlMessage | None = None  # type: ignore [name-defined]
