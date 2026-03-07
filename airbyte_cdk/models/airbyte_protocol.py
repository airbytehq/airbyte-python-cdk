#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Annotated, Any, Dict, List, Mapping, Optional, Union

from airbyte_protocol_dataclasses.models import *  # noqa: F403  # Allow '*'
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


# The following dataclasses have been redeclared to include scopes, optional_scopes,
# and scopes_join_strategy fields that are used by declarative OAuth connectors.
# The protocol model (OauthConnectorInputSpecification) does not include these fields,
# so serpyco_rs silently drops them during deserialization. By overriding the model here
# and cascading through OAuthConfigSpecification → AdvancedAuth → ConnectorSpecification,
# the fields are preserved in the connector's spec output.
# This follows the same override pattern used above for AirbyteStateBlob.
@dataclass
class OauthConnectorInputSpecification:
    consent_url: str
    access_token_url: str
    scope: Optional[str] = None
    scopes: Optional[List[Dict[str, Any]]] = None
    optional_scopes: Optional[List[Dict[str, Any]]] = None
    scopes_join_strategy: Optional[str] = None
    access_token_headers: Optional[Dict[str, Any]] = None
    access_token_params: Optional[Dict[str, Any]] = None
    extract_output: Optional[List[str]] = None
    state: Optional[State] = None  # type: ignore [name-defined]
    client_id_key: Optional[str] = None
    client_secret_key: Optional[str] = None
    scope_key: Optional[str] = None
    state_key: Optional[str] = None
    auth_code_key: Optional[str] = None
    redirect_uri_key: Optional[str] = None
    token_expiry_key: Optional[str] = None


@dataclass
class OAuthConfigSpecification:
    oauth_user_input_from_connector_config_specification: Optional[Dict[str, Any]] = None
    oauth_connector_input_specification: Optional[OauthConnectorInputSpecification] = None
    complete_oauth_output_specification: Optional[Dict[str, Any]] = None
    complete_oauth_server_input_specification: Optional[Dict[str, Any]] = None
    complete_oauth_server_output_specification: Optional[Dict[str, Any]] = None


@dataclass
class AdvancedAuth:
    auth_flow_type: Optional[AuthFlowType] = None  # type: ignore [name-defined]
    predicate_key: Optional[List[str]] = None
    predicate_value: Optional[str] = None
    oauth_config_specification: Optional[OAuthConfigSpecification] = None


@dataclass
class ConnectorSpecification:
    connectionSpecification: Dict[str, Any]
    documentationUrl: Optional[str] = None
    changelogUrl: Optional[str] = None
    supportsIncremental: Optional[bool] = None
    supportsNormalization: Optional[bool] = False
    supportsDBT: Optional[bool] = False
    supported_destination_sync_modes: Optional[List[DestinationSyncMode]] = None  # type: ignore [name-defined]
    authSpecification: Optional[AuthSpecification] = None  # type: ignore [name-defined]
    advanced_auth: Optional[AdvancedAuth] = None
    protocol_version: Optional[str] = None


@dataclass
class AirbyteMessage:
    type: Type  # type: ignore [name-defined]
    log: Optional[AirbyteLogMessage] = None  # type: ignore [name-defined]
    spec: Optional[ConnectorSpecification] = None
    connectionStatus: Optional[AirbyteConnectionStatus] = None  # type: ignore [name-defined]
    catalog: Optional[AirbyteCatalog] = None  # type: ignore [name-defined]
    record: Optional[AirbyteRecordMessage] = None  # type: ignore [name-defined]
    state: Optional[AirbyteStateMessage] = None
    trace: Optional[AirbyteTraceMessage] = None  # type: ignore [name-defined]
    control: Optional[AirbyteControlMessage] = None  # type: ignore [name-defined]
