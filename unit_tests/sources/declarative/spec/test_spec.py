#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

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
from airbyte_cdk.sources.declarative.spec.spec import Spec as component_spec


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
