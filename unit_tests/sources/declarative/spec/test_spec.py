#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest
from airbyte_cdk.models import AdvancedAuth, AuthFlowType, ConnectorSpecification
from airbyte_cdk.sources.declarative.models.declarative_component_schema import AuthFlow
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    OAuthConfigSpecification,
    OauthConnectorInputSpecification,
    State,
)
from airbyte_cdk.sources.declarative.spec.spec import Spec


@pytest.mark.parametrize(
    "spec, expected_connection_specification",
    [
        (
            Spec(connection_specification={"client_id": "my_client_id"}, parameters={}),
            ConnectorSpecification(connectionSpecification={"client_id": "my_client_id"}),
        ),
        (
            Spec(
                connection_specification={"client_id": "my_client_id"},
                parameters={},
                documentation_url="https://airbyte.io",
            ),
            ConnectorSpecification(
                connectionSpecification={"client_id": "my_client_id"},
                documentationUrl="https://airbyte.io",
            ),
        ),
        (
            Spec(
                connection_specification={"client_id": "my_client_id"},
                parameters={},
                advanced_auth=AuthFlow(
                    auth_flow_type="oauth2.0",
                ),
            ),
            ConnectorSpecification(
                connectionSpecification={"client_id": "my_client_id"},
                advanced_auth=AdvancedAuth(auth_flow_type=AuthFlowType.oauth2_0),
            ),
        ),
    ],
    ids=[
        "test_only_connection_specification",
        "test_with_doc_url",
        "test_auth_flow",
    ],
)
def test_spec(spec, expected_connection_specification):
    assert spec.generate_spec() == expected_connection_specification


@pytest.mark.parametrize(
    "input_oauth_object, expected_spec",
    [
        (
            OAuthConfigSpecification(
                oauth_connector_input_specification=OauthConnectorInputSpecification(
                    consent_url="https://domain.host.com/endpoint/oauth?{client_id_key}={{client_id_key}}&{redirect_uri_key}={urlEncoder:{{redirect_uri_key}}}&{state_key}={{state_key}}",
                    scope="reports:read campaigns:read",
                    access_token_headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    access_token_params={
                        "{auth_code_key}": "{{auth_code_key}}",
                        "{client_id_key}": "{{client_id_key}}",
                        "{client_secret_key}": "{{client_secret_key}}",
                    },
                    access_token_url="https://domain.host.com/endpoint/v1/oauth2/access_token/",
                    extract_output=["data.access_token"],
                    state=State(min=7, max=27),
                    client_id_key="my_client_id_key",
                    client_secret_key="my_client_secret_key",
                    scope_key="my_scope_key",
                    state_key="my_state_key",
                    auth_code_key="my_auth_code_key",
                    redirect_uri_key="callback_uri",
                )
            ),
            {
                "oauth_user_input_from_connector_config_specification": None,
                "oauth_connector_input_specification": {
                    "consent_url": "https://domain.host.com/endpoint/oauth?{client_id_key}={{client_id_key}}&{redirect_uri_key}={urlEncoder:{{redirect_uri_key}}}&{state_key}={{state_key}}",
                    "scope": "reports:read campaigns:read",
                    "access_token_url": "https://domain.host.com/endpoint/v1/oauth2/access_token/",
                    "access_token_headers": {
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    "access_token_params": {
                        "{auth_code_key}": "{{auth_code_key}}",
                        "{client_id_key}": "{{client_id_key}}",
                        "{client_secret_key}": "{{client_secret_key}}",
                    },
                    "extract_output": ["data.access_token"],
                    "state": {"min": 7, "max": 27},
                    "client_id_key": "my_client_id_key",
                    "client_secret_key": "my_client_secret_key",
                    "scope_key": "my_scope_key",
                    "state_key": "my_state_key",
                    "auth_code_key": "my_auth_code_key",
                    "redirect_uri_key": "callback_uri",
                },
                "complete_oauth_output_specification": None,
                "complete_oauth_server_input_specification": None,
                "complete_oauth_server_output_specification": None,
            },
        )
    ],
    ids=["test_declarative_oauth_flow"],
)
def test_with_declarative_oauth_flow_spec(input_oauth_object, expected_spec) -> None:
    assert input_oauth_object.dict() == expected_spec
