# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from __future__ import annotations

import json
from dataclasses import InitVar, dataclass
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

import orjson
from airbyte_protocol_dataclasses.models import (
    AdvancedAuth,
    AirbyteAnalyticsTraceMessage,
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteControlConnectorConfigMessage,
    AirbyteControlMessage,
    AirbyteErrorTraceMessage,
    AirbyteEstimateTraceMessage,
    AirbyteGlobalState,
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteProtocol,
    AirbyteRecordMessage,
    AirbyteRecordMessageFileReference,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateStats,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    AirbyteStreamStatus,
    AirbyteStreamStatusReason,
    AirbyteStreamStatusReasonType,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    AuthFlowType,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    EstimateType,
    FailureType,
    Level,
    OAuthConfigSpecification,
    OauthConnectorInputSpecification,
    OrchestratorType,
    State,
    Status,
    StreamDescriptor,
    SyncMode,
    TraceType,
    Type,
)
from boltons.typeutils import classproperty
from serpyco_rs import CustomType, Serializer
from serpyco_rs.metadata import Alias

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


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


T = TypeVar("T", bound="SerDeMixin")


def _custom_state_resolver(t: type) -> CustomType[AirbyteStateBlob, dict[str, Any]] | None:
    class AirbyteStateBlobType(CustomType[AirbyteStateBlob, dict[str, Any]]):
        def serialize(self, value: AirbyteStateBlob) -> dict[str, Any]:
            # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
            return {k: v for k, v in value.__dict__.items()}

        def deserialize(self, value: dict[str, Any]) -> AirbyteStateBlob:
            return AirbyteStateBlob(value)

        def get_json_schema(self) -> dict[str, Any]:
            return {"type": "object"}

    return AirbyteStateBlobType() if t is AirbyteStateBlob else None


class SerDeMixin:
    _serializer: Serializer[Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the object to a dictionary.

        This method uses the `Serializer` to serialize the object to a dict as quickly as possible.
        """
        return self._serializer.dump(self)

    def to_json(self) -> str:
        """Serialize the object to JSON.

        This method uses `orjson` to serialize the object to JSON as quickly as possible.
        """
        return orjson.dumps(self.to_dict()).decode("utf-8")

    def __str__(self) -> str:
        """Casting to `str` is the same as casting to JSON.

        These are equivalent:
        >>> msg = AirbyteMessage(...)
        >>> str(msg)
        >>> msg.to_json()
        """
        return self.to_json()

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any], /) -> T:
        return cls._serializer.load(data)

    @classmethod
    def from_json(cls: type[T], str_value: str, /) -> T:
        """Load the object from JSON.

        This method first tries to deserialize the JSON string using `orjson.loads()`,
        falling back to `json.loads()` if it fails. This is because `orjson` does not support
        all JSON features, such as `NaN` and `Infinity`, which are supported by the standard
        `json` module. The `orjson` library is used for its speed and efficiency, while the
        standard `json` library is used as a fallback for compatibility with more complex JSON
        structures.

        Raises:
            orjson.JSONDecodeError: If the JSON string cannot be deserialized by either
            `orjson` or `json`.
        """
        try:
            dict_value = orjson.loads(str_value)
        except orjson.JSONDecodeError as orjson_error:
            try:
                dict_value = json.loads(str_value)
            except json.JSONDecodeError as json_error:
                # Callers will expect `orjson.JSONDecodeError`, so we raise the original
                # `orjson` error when both options fail.
                # We also attach the second error, in case it is useful for debugging.
                raise orjson_error from json_error

        return cls.from_dict(dict_value)


# The following dataclasses have been redeclared to include the new version of AirbyteStateBlob
@dataclass
class AirbyteStreamState(AirbyteStreamState, SerDeMixin):
    stream_descriptor: StreamDescriptor  # type: ignore [name-defined]
    stream_state: Optional[AirbyteStateBlob] = None


AirbyteStreamState._serializer = Serializer(
    AirbyteStreamState,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)


@dataclass
class AirbyteGlobalState(SerDeMixin):
    stream_states: List[AirbyteStreamState]
    shared_state: Optional[AirbyteStateBlob] = None


AirbyteGlobalState._serializer = Serializer(
    AirbyteGlobalState,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)

@dataclass
class AirbyteStateMessage(SerDeMixin):
    type: AirbyteStateType | None = None  # type: ignore [name-defined]
    stream: Optional[AirbyteStreamState] = None
    global_: Annotated[AirbyteGlobalState | None, Alias("global")] = (
        None  # "global" is a reserved keyword in python ⇒ Alias is used for (de-)serialization
    )
    data: Optional[Dict[str, Any]] = None
    sourceStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]
    destinationStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]


AirbyteStateMessage._serializer = Serializer(
    AirbyteStateMessage,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)


@dataclass
class AirbyteMessage(SerDeMixin):
    type: Type  # type: ignore [name-defined]
    log: Optional[AirbyteLogMessage] = None  # type: ignore [name-defined]
    spec: Optional[ConnectorSpecification] = None  # type: ignore [name-defined]
    connectionStatus: Optional[AirbyteConnectionStatus] = None  # type: ignore [name-defined]
    catalog: Optional[AirbyteCatalog] = None  # type: ignore [name-defined]
    record: Optional[AirbyteRecordMessage] = None  # type: ignore [name-defined]
    state: Optional[AirbyteStateMessage] = None
    trace: Optional[AirbyteTraceMessage] = None  # type: ignore [name-defined]
    control: Optional[AirbyteControlMessage] = None  # type: ignore [name-defined]


AirbyteMessage._serializer = Serializer(
    AirbyteMessage,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)


class ConfiguredAirbyteCatalog(ConfiguredAirbyteCatalog, SerDeMixin):
    pass


ConfiguredAirbyteCatalog._serializer = Serializer(
    ConfiguredAirbyteCatalog,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)


class ConfiguredAirbyteStream(ConfiguredAirbyteStream, SerDeMixin):
    pass


ConfiguredAirbyteStream._serializer = Serializer(
    ConfiguredAirbyteStream,
    omit_none=True,
    custom_type_resolver=_custom_state_resolver,
)


class ConnectorSpecification(ConnectorSpecification, SerDeMixin):
    pass


ConnectorSpecification._serializer = Serializer(
    ConnectorSpecification,
    omit_none=True,
)

# Deprecated Serializer Classes. Declared here for legacy compatibility:
AirbyteStreamStateSerializer = AirbyteStreamState._serializer  # type: ignore
AirbyteStateMessageSerializer = AirbyteStateMessage._serializer  # type: ignore
AirbyteMessageSerializer = AirbyteMessage._serializer  # type: ignore
ConfiguredAirbyteCatalogSerializer = ConfiguredAirbyteCatalog._serializer  # type: ignore
ConfiguredAirbyteStreamSerializer = ConfiguredAirbyteStream._serializer  # type: ignore
ConnectorSpecificationSerializer = ConnectorSpecification._serializer  # type: ignore
