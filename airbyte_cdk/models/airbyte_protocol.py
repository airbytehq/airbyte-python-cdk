# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
from collections.abc import Callable, Mapping
from dataclasses import InitVar, dataclass
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

import orjson
from airbyte_protocol_dataclasses.models import *  # noqa: F403  # Allow '*'
from serpyco_rs import CustomType, Serializer
from serpyco_rs.metadata import Alias


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


class SerDeMixin:
    # allow subclasses to override their resolver if they need one
    _type_resolver: Callable[[type], CustomType[Any, Any] | None] | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # build a Serializer *once* for each subclass
        cls._serializer = Serializer(
            cls,
            omit_none=True,
            custom_type_resolver=cls._type_resolver,
        )

    def to_dict(self) -> Dict[str, Any]:
        return self._serializer.dump(self)

    def to_json(self) -> str:
        # use to_dict so you only have one canonical dump
        return orjson.dumps(self.to_dict()).decode("utf-8")

    @classmethod
    def from_dict(cls: type[T], data: Dict[str, Any]) -> T:
        return cls._serializer.load(data)

    @classmethod
    def from_json(cls: type[T], s: str) -> T:
        return cls._serializer.load(orjson.loads(s))


def _custom_state_resolver(t: type) -> CustomType[AirbyteStateBlob, dict[str, Any]] | None:
    class AirbyteStateBlobType(CustomType[AirbyteStateBlob, Dict[str, Any]]):
        def serialize(self, value: AirbyteStateBlob) -> Dict[str, Any]:
            # cant use orjson.dumps() directly because private attributes are excluded, e.g. "__ab_full_refresh_sync_complete"
            return {k: v for k, v in value.__dict__.items()}

        def deserialize(self, value: Dict[str, Any]) -> AirbyteStateBlob:
            return AirbyteStateBlob(value)

        def get_json_schema(self) -> Dict[str, Any]:
            return {"type": "object"}

    return AirbyteStateBlobType() if t is AirbyteStateBlob else None


# The following dataclasses have been redeclared to include the new version of AirbyteStateBlob
@dataclass
class AirbyteStreamState(SerDeMixin):
    stream_descriptor: StreamDescriptor  # type: ignore [name-defined]
    stream_state: Optional[AirbyteStateBlob] = None

    # override the resolver for AirbyteStreamState to use the custom one
    _type_resolver = _custom_state_resolver


@dataclass
class AirbyteGlobalState(SerDeMixin):
    stream_states: List[AirbyteStreamState]
    shared_state: Optional[AirbyteStateBlob] = None

    # override the resolver for AirbyteStreamState to use the custom one
    _type_resolver = _custom_state_resolver


@dataclass
class AirbyteStateMessage(SerDeMixin):
    type: Optional[AirbyteStateType] = None  # type: ignore [name-defined]
    stream: Optional[AirbyteStreamState] = None
    global_: Annotated[AirbyteGlobalState | None, Alias("global")] = (
        None  # "global" is a reserved keyword in python ⇒ Alias is used for (de-)serialization
    )
    data: Optional[Dict[str, Any]] = None
    sourceStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]
    destinationStats: Optional[AirbyteStateStats] = None  # type: ignore [name-defined]

    # override the resolver for AirbyteStreamState to use the custom one
    _type_resolver = _custom_state_resolver


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

    # override the resolver for AirbyteStreamState to use the custom one
    _type_resolver = _custom_state_resolver


# These don't need the custom resolver:
class ConnectorSpecification(ConnectorSpecification, SerDeMixin):
    pass


class ConfiguredAirbyteCatalog(ConfiguredAirbyteCatalog, SerDeMixin):
    pass


class AirbyteStream(AirbyteStream, SerDeMixin):
    pass
