#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import dataclasses
from typing import Any, List, Mapping

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
    Spec as SpecModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    State as component_declarative_oauth_state,
)
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
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
                            scopes_join_strategy="space",  # default from component schema, preserved through protocol override
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


@pytest.mark.parametrize(
    "upstream_class_name, override_class_name",
    [
        ("OauthConnectorInputSpecification", "OauthConnectorInputSpecification"),
        ("OAuthConfigSpecification", "OAuthConfigSpecification"),
        ("AdvancedAuth", "AdvancedAuth"),
        ("ConnectorSpecification", "ConnectorSpecification"),
    ],
    ids=[
        "OauthConnectorInputSpecification",
        "OAuthConfigSpecification",
        "AdvancedAuth",
        "ConnectorSpecification",
    ],
)
def test_protocol_override_fields_in_sync(
    upstream_class_name: str, override_class_name: str
) -> None:
    """Ensure protocol override dataclasses stay compatible with their upstream counterparts.

    The airbyte_protocol.py file redeclares several dataclasses to add fields that the
    upstream airbyte_protocol_dataclasses package does not yet include (e.g. scopes,
    optional_scopes, scopes_join_strategy). If the upstream package adds new fields, this
    test will fail to remind us to update the local overrides.
    """
    import airbyte_protocol_dataclasses.models as upstream_models

    import airbyte_cdk.models.airbyte_protocol as override_models

    upstream_cls = getattr(upstream_models, upstream_class_name)
    override_cls = getattr(override_models, override_class_name)

    upstream_fields = {f.name for f in dataclasses.fields(upstream_cls)}
    override_fields = {f.name for f in dataclasses.fields(override_cls)}

    missing = upstream_fields - override_fields
    assert not missing, (
        f"Upstream protocol added fields {missing} to {upstream_class_name} "
        f"that are missing from the airbyte_protocol.py override. Update the override to match."
    )


def _create_spec_with_validations(validations: List[Mapping[str, Any]]) -> component_spec:
    return ModelToComponentFactory().create_component(
        model_type=SpecModel,
        component_definition={
            "type": "Spec",
            "connection_specification": {},
            "config_normalization_rules": {
                "type": "ConfigNormalizationRules",
                "validations": validations,
            },
        },
        config={},
    )


_UNIQUE_STREAM_NAMES_VALIDATION = {
    "type": "PredicateValidator",
    "value": "{{ config['report_options_list'] | map(attribute='stream_name') | list }}",
    "validation_strategy": {
        "type": "ValidateAdheresToSchema",
        "base_schema": {"type": "array", "uniqueItems": True},
    },
}

_JIRA_DOMAIN_VALIDATION = {
    "type": "DpathValidator",
    "field_path": ["domain"],
    "validation_strategy": {
        "type": "ValidateAdheresToSchema",
        "base_schema": {"type": "string", "pattern": "^[a-zA-Z0-9-]+\\.atlassian\\.net$"},
    },
}


def test_given_predicate_validator_with_unique_interpolated_list_when_validate_config_then_succeeds() -> (
    None
):
    spec = _create_spec_with_validations([_UNIQUE_STREAM_NAMES_VALIDATION])

    spec.validate_config(
        {"report_options_list": [{"stream_name": "orders"}, {"stream_name": "returns"}]}
    )


def test_given_predicate_validator_with_duplicate_interpolated_list_when_validate_config_then_raises() -> (
    None
):
    spec = _create_spec_with_validations([_UNIQUE_STREAM_NAMES_VALIDATION])

    with pytest.raises(ValueError, match="non-unique elements"):
        spec.validate_config(
            {"report_options_list": [{"stream_name": "orders"}, {"stream_name": "orders"}]}
        )


def test_given_predicate_validator_reading_transformed_config_when_validate_config_then_uses_passed_config() -> (
    None
):
    spec = _create_spec_with_validations(
        [
            {
                "type": "PredicateValidator",
                "value": "{{ config['added_field'] }}",
                "validation_strategy": {
                    "type": "ValidateAdheresToSchema",
                    "base_schema": {"type": "string", "const": "expected"},
                },
            }
        ]
    )

    spec.validate_config({"added_field": "expected"})
    with pytest.raises(ValueError):
        spec.validate_config({"added_field": "unexpected"})


@pytest.mark.parametrize(
    "domain, is_valid",
    [
        pytest.param("example.atlassian.net", True, id="valid_hostname"),
        pytest.param("https://example.atlassian.net/", False, id="url_instead_of_hostname"),
        pytest.param("example.com", False, id="non_atlassian_hostname"),
    ],
)
def test_given_dpath_validator_with_plain_string_pattern_when_validate_config(
    domain: str, is_valid: bool
) -> None:
    spec = _create_spec_with_validations([_JIRA_DOMAIN_VALIDATION])

    if is_valid:
        spec.validate_config({"domain": domain})
    else:
        with pytest.raises(ValueError, match="does not match"):
            spec.validate_config({"domain": domain})
