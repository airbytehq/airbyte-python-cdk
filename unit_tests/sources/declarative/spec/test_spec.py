#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock, mock_open

import pytest

from airbyte_cdk.models import (
    AdvancedAuth as model_advanced_auth,
)
from airbyte_cdk.models import (
    AuthFlowType as model_auth_flow_type,
)
from airbyte_cdk.models import (
    ConnectorSpecification as model_connector_spec,
)
from airbyte_cdk.models import (
    OAuthConfigSpecification as model_declarative_oauth_spec,
)
from airbyte_cdk.models import (
    OauthConnectorInputSpecification as model_declarative_oauth_connector_input_spec,
)
from airbyte_cdk.models import (
    State as model_declarative_oauth_state,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    AuthFlow as component_auth_flow,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    AuthFlowType as component_auth_flow_type,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    OAuthConfigSpecification as component_declarative_oauth_config_spec,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    OauthConnectorInputSpecification as component_declarative_oauth_connector_input_spec,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    State as component_declarative_oauth_state,
)
from airbyte_cdk.sources.declarative.spec.spec import ConfigMigration
from airbyte_cdk.sources.declarative.spec.spec import Spec as component_spec
from airbyte_cdk.sources.declarative.transformations.add_fields import AddedFieldDefinition
from airbyte_cdk.sources.declarative.transformations.config_transformations.add_fields import (
    ConfigAddFields,
)
from airbyte_cdk.sources.declarative.transformations.config_transformations.remap_field import (
    ConfigRemapField,
)
from airbyte_cdk.sources.declarative.transformations.config_transformations.remove_fields import (
    ConfigRemoveFields,
)
from airbyte_cdk.sources.declarative.validators.dpath_validator import DpathValidator
from airbyte_cdk.sources.declarative.validators.validate_adheres_to_schema import (
    ValidateAdheresToSchema,
)


@pytest.mark.parametrize(
    "spec, expected_connection_specification",
    [
        (
            component_spec(connection_specification={"client_id": "my_client_id"}, parameters={}),
            model_connector_spec(connectionSpecification={"client_id": "my_client_id"}),
        ),
        (
            component_spec(
                connection_specification={"client_id": "my_client_id"},
                parameters={},
                documentation_url="https://airbyte.io",
            ),
            model_connector_spec(
                connectionSpecification={"client_id": "my_client_id"},
                documentationUrl="https://airbyte.io",
            ),
        ),
        (
            component_spec(
                connection_specification={"client_id": "my_client_id"},
                parameters={},
                advanced_auth=component_auth_flow(
                    auth_flow_type=component_auth_flow_type.oauth2_0,
                    predicate_key=None,
                    predicate_value=None,
                ),
            ),
            model_connector_spec(
                connectionSpecification={"client_id": "my_client_id"},
                advanced_auth=model_advanced_auth(
                    auth_flow_type=model_auth_flow_type.oauth2_0,
                ),
            ),
        ),
        (
            component_spec(
                connection_specification={},
                parameters={},
                advanced_auth=component_auth_flow(
                    auth_flow_type=component_auth_flow_type.oauth2_0,
                    predicate_key=None,
                    predicate_value=None,
                    oauth_config_specification=component_declarative_oauth_config_spec(
                        oauth_connector_input_specification=component_declarative_oauth_connector_input_spec(
                            consent_url="https://domain.host.com/endpoint/oauth?{client_id_key}={{client_id_key}}&{redirect_uri_key}={urlEncoder:{{redirect_uri_key}}}&{state_key}={{state_key}}",
                            scope="reports:read campaigns:read",
                            access_token_headers={"Content-Type": "application/json"},
                            access_token_params={"{auth_code_key}": "{{auth_code_key}}"},
                            access_token_url="https://domain.host.com/endpoint/v1/oauth2/access_token/",
                            extract_output=["data.access_token"],
                            state=component_declarative_oauth_state(min=7, max=27),
                            client_id_key="my_client_id_key",
                            client_secret_key="my_client_secret_key",
                            scope_key="my_scope_key",
                            state_key="my_state_key",
                            auth_code_key="my_auth_code_key",
                            redirect_uri_key="callback_uri",
                        ),
                        oauth_user_input_from_connector_config_specification=None,
                        complete_oauth_output_specification=None,
                        complete_oauth_server_input_specification=None,
                        complete_oauth_server_output_specification=None,
                    ),
                ),
            ),
            model_connector_spec(
                connectionSpecification={},
                advanced_auth=model_advanced_auth(
                    auth_flow_type=model_auth_flow_type.oauth2_0,
                    predicate_key=None,
                    predicate_value=None,
                    oauth_config_specification=model_declarative_oauth_spec(
                        oauth_connector_input_specification=model_declarative_oauth_connector_input_spec(
                            consent_url="https://domain.host.com/endpoint/oauth?{client_id_key}={{client_id_key}}&{redirect_uri_key}={urlEncoder:{{redirect_uri_key}}}&{state_key}={{state_key}}",
                            scope="reports:read campaigns:read",
                            access_token_headers={"Content-Type": "application/json"},
                            access_token_params={"{auth_code_key}": "{{auth_code_key}}"},
                            access_token_url="https://domain.host.com/endpoint/v1/oauth2/access_token/",
                            extract_output=["data.access_token"],
                            state=model_declarative_oauth_state(min=7, max=27),
                            client_id_key="my_client_id_key",
                            client_secret_key="my_client_secret_key",
                            scope_key="my_scope_key",
                            state_key="my_state_key",
                            auth_code_key="my_auth_code_key",
                            redirect_uri_key="callback_uri",
                        ),
                    ),
                ),
            ),
        ),
    ],
    ids=[
        "test_only_connection_specification",
        "test_with_doc_url",
        "test_auth_flow",
        "test_declarative_oauth_flow",
    ],
)
def test_spec(spec, expected_connection_specification) -> None:
    assert spec.generate_spec() == expected_connection_specification


@pytest.fixture
def migration_mocks(monkeypatch):
    mock_message_repository = Mock()
    mock_message_repository.consume_queue.return_value = [Mock()]

    mock_source = Mock()
    mock_entrypoint = Mock()
    mock_entrypoint.extract_config.return_value = "/fake/config/path"
    monkeypatch.setattr(
        "airbyte_cdk.sources.declarative.spec.spec.AirbyteEntrypoint", lambda _: mock_entrypoint
    )

    _mock_open = mock_open()
    mock_json_dump = Mock()
    mock_print = Mock()
    mock_serializer_dump = Mock()

    mock_decoded_bytes = Mock()
    mock_decoded_bytes.decode.return_value = "decoded_message"
    mock_orjson_dumps = Mock(return_value=mock_decoded_bytes)

    monkeypatch.setattr("builtins.open", _mock_open)
    monkeypatch.setattr("json.dump", mock_json_dump)
    monkeypatch.setattr("builtins.print", mock_print)
    monkeypatch.setattr(
        "airbyte_cdk.models.airbyte_protocol_serializers.AirbyteMessageSerializer.dump",
        mock_serializer_dump,
    )
    monkeypatch.setattr("airbyte_cdk.sources.declarative.spec.spec.orjson.dumps", mock_orjson_dumps)

    return {
        "message_repository": mock_message_repository,
        "source": mock_source,
        "open": _mock_open,
        "json_dump": mock_json_dump,
        "print": mock_print,
        "serializer_dump": mock_serializer_dump,
        "orjson_dumps": mock_orjson_dumps,
        "decoded_bytes": mock_decoded_bytes,
    }


def test_given_unmigrated_config_when_migrating_then_config_is_migrated(migration_mocks) -> None:
    input_config = {"planet": "CRSC"}

    spec = component_spec(
        connection_specification={},
        parameters={},
        config_migrations=[
            ConfigMigration(
                description="Test migration",
                transformations=[
                    ConfigRemapField(
                        map={"CRSC": "Coruscant"}, field_path=["planet"], config=input_config
                    )
                ],
            )
        ],
    )
    spec.message_repository = migration_mocks["message_repository"]

    spec.migrate_config("/fake/config/path", input_config)

    migration_mocks["message_repository"].emit_message.assert_called_once()
    migration_mocks["open"].assert_called_once_with("/fake/config/path", "w")
    migration_mocks["json_dump"].assert_called_once()
    migration_mocks["print"].assert_called()
    migration_mocks["serializer_dump"].assert_called()
    migration_mocks["orjson_dumps"].assert_called()
    migration_mocks["decoded_bytes"].decode.assert_called()


def test_given_already_migrated_config_no_control_message_is_emitted(migration_mocks) -> None:
    input_config = {"planet": "Coruscant"}

    spec = component_spec(
        connection_specification={},
        parameters={},
        config_migrations=[
            ConfigMigration(
                description="Test migration",
                transformations=[
                    ConfigRemapField(
                        map={"CRSC": "Coruscant"}, field_path=["planet"], config=input_config
                    )
                ],
            )
        ],
    )
    spec.message_repository = migration_mocks["message_repository"]

    spec.migrate_config("/fake/config/path", input_config)

    migration_mocks["message_repository"].emit_message.assert_not_called()
    migration_mocks["open"].assert_not_called()
    migration_mocks["json_dump"].assert_not_called()
    migration_mocks["print"].assert_not_called()
    migration_mocks["serializer_dump"].assert_not_called()
    migration_mocks["orjson_dumps"].assert_not_called()
    migration_mocks["decoded_bytes"].decode.assert_not_called()


def test_given_list_of_transformations_when_transform_config_then_config_is_transformed() -> None:
    input_config = {"planet_code": "CRSC"}
    expected_config = {
        "planet_name": "Coruscant",
        "planet_population": 3_000_000_000_000,
    }
    spec = component_spec(
        connection_specification={},
        parameters={},
        config_transformations=[
            ConfigAddFields(
                fields=[
                    AddedFieldDefinition(
                        path=["planet_name"],
                        value="{{ config['planet_code'] }}",
                        value_type=None,
                        parameters={},
                    ),
                    AddedFieldDefinition(
                        path=["planet_population"],
                        value="{{ config['planet_code'] }}",
                        value_type=None,
                        parameters={},
                    ),
                ]
            ),
            ConfigRemapField(
                map={
                    "CRSC": "Coruscant",
                },
                field_path=["planet_name"],
                config=input_config,
            ),
            ConfigRemapField(
                map={
                    "CRSC": 3_000_000_000_000,
                },
                field_path=["planet_population"],
                config=input_config,
            ),
            ConfigRemoveFields(
                field_pointers=["planet_code"],
            ),
        ],
    )
    spec.transform_config(input_config)

    assert input_config == expected_config


def test_given_valid_config_value_when_validating_then_no_exception_is_raised() -> None:
    spec = component_spec(
        connection_specification={},
        parameters={},
        config_validations=[
            DpathValidator(
                field_path=["test_field"],
                strategy=ValidateAdheresToSchema(
                    schema={
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "Test Spec",
                        "type": "object",
                        "required": [],
                        "additionalProperties": False,
                        "properties": {
                            "field_to_validate": {
                                "type": "string",
                                "title": "Name",
                                "description": "The name of the test spec",
                                "airbyte_secret": False,
                            }
                        },
                    }
                ),
            )
        ],
    )
    input_config = {"test_field": {"field_to_validate": "test"}}
    spec.validate_config(input_config)


def test_given_invalid_config_value_when_validating_then_exception_is_raised() -> None:
    spec = component_spec(
        connection_specification={},
        parameters={},
        config_validations=[
            DpathValidator(
                field_path=["test_field"],
                strategy=ValidateAdheresToSchema(
                    schema={
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "Test Spec",
                        "type": "object",
                        "required": [],
                        "properties": {
                            "field_to_validate": {
                                "type": "string",
                                "title": "Name",
                                "description": "The name of the test spec",
                                "airbyte_secret": False,
                            }
                        },
                    }
                ),
            )
        ],
    )
    input_config = {"test_field": {"field_to_validate": 123}}

    with pytest.raises(Exception):
        spec.validate_config(input_config)
