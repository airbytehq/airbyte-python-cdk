#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

# Serpyco uses type definitions at runtime during SerDes operations.
# For this reason we have some exceptions to normal linting rules.
# ruff: noqa: TCH001, TCH002, TCH003  # Don't auto-move imports to `TYPE_CHECKING` block.
# ruff: noqa: F403    # Allow '*' import to shadow everything from protocols package.

# Allow camelCase names (imported from java library)
# ruff: noqa: N815

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import InitVar, dataclass

# Serpyco does not support the 3.10-style "|" operator.
# ruff: noqa: UP007  # Allow deprecated `Union` and `Optional`
from typing import Annotated, Any, Optional, Union

from serpyco_rs.metadata import Alias

from airbyte_protocol_dataclasses import models
from airbyte_protocol_dataclasses.models import *

from airbyte_cdk.models.file_transfer_record_message import AirbyteFileTransferRecordMessage


@dataclass
class AirbyteStateBlob:  # noqa: PLW1641  # Should implement __hash__
    """A dataclass that dynamically sets attributes based on provided keyword arguments and positional arguments.
    Used to "mimic" pydantic BaseModel with ConfigDict(extra='allow') option.

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

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401 (any-type)
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
    stream_descriptor: models.StreamDescriptor
    stream_state: Optional[AirbyteStateBlob] = None


@dataclass
class AirbyteGlobalState:
    stream_states: list[AirbyteStreamState]
    shared_state: Optional[AirbyteStateBlob] = None


@dataclass
class AirbyteStateMessage:
    type: Optional[models.AirbyteStateType] = None

    # These two use custom classes defined above
    stream: Optional[AirbyteStreamState] = None
    global_: Annotated[Optional[AirbyteGlobalState], Alias("global")] = (
        None  # "global" is a reserved keyword in python ⇒ Alias is used for (de-)serialization
    )

    data: Optional[dict[str, Any]] = None
    sourceStats: Optional[models.AirbyteStateStats] = None
    destinationStats: Optional[models.AirbyteStateStats] = None


@dataclass
class AirbyteMessage:
    type: models.Type
    log: Optional[models.AirbyteLogMessage] = None
    spec: Optional[models.ConnectorSpecification] = None
    connectionStatus: Optional[models.AirbyteConnectionStatus] = None
    catalog: Optional[models.AirbyteCatalog] = None

    # These two differ from the original dataclasses:
    record: Optional[Union[AirbyteFileTransferRecordMessage, models.AirbyteRecordMessage]] = None
    state: Optional[AirbyteStateMessage] = None

    trace: Optional[models.AirbyteTraceMessage] = None
    control: Optional[models.AirbyteControlMessage] = None
