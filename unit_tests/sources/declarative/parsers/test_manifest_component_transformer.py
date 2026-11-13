#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.sources.declarative.parsers.manifest_component_transformer import (
    ManifestComponentTransformer,
)


@pytest.mark.parametrize(
    "component, expected_component",
    [
        pytest.param(
            {
                "type": "DeclarativeSource",
                "streams": [{"type": "DeclarativeStream", "retriever": {}, "schema_loader": {}}],
            },
            {
                "type": "DeclarativeSource",
                "streams": [
                    {
                        "type": "DeclarativeStream",
                        "retriever": {"type": "SimpleRetriever"},
                        "schema_loader": {"type": "JsonFileSchemaLoader"},
                    }
                ],
            },
            id="test_declarative_stream",
        ),
        pytest.param(
            {
                "type": "DeclarativeStream",
                "retriever": {
                    "type": "SimpleRetriever",
                    "paginator": {},
                    "record_selector": {},
                    "requester": {},
                },
            },
            {
                "type": "DeclarativeStream",
                "retriever": {
                    "type": "SimpleRetriever",
                    "paginator": {"type": "NoPagination"},
                    "record_selector": {"type": "RecordSelector"},
                    "requester": {"type": "HttpRequester"},
                },
            },
            id="test_simple_retriever",
        ),
        pytest.param(
            {
                "type": "DeclarativeStream",
                "requester": {"type": "HttpRequester", "error_handler": {}},
            },
            {
                "type": "DeclarativeStream",
                "requester": {
                    "type": "HttpRequester",
                    "error_handler": {"type": "DefaultErrorHandler"},
                },
            },
            id="test_http_requester",
        ),
        pytest.param(
            {
                "type": "SimpleRetriever",
                "paginator": {
                    "type": "DefaultPaginator",
                    "page_size_option": {},
                    "page_token_option": {},
                },
            },
            {
                "type": "SimpleRetriever",
                "paginator": {
                    "type": "DefaultPaginator",
                    "page_size_option": {"type": "RequestOption"},
                    "page_token_option": {},
                },
            },
            id="test_default_paginator",
        ),
        pytest.param(
            {
                "type": "SimpleRetriever",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [{}, {}, {}],
                },
            },
            {
                "type": "SimpleRetriever",
                "partition_router": {
                    "type": "SubstreamPartitionRouter",
                    "parent_stream_configs": [
                        {"type": "ParentStreamConfig"},
                        {"type": "ParentStreamConfig"},
                        {"type": "ParentStreamConfig"},
                    ],
                },
            },
            id="test_substream_slicer",
        ),
    ],
)
def test_find_default_types(component, expected_component):
    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


@pytest.mark.parametrize(
    "component, expected_component",
    [
        pytest.param(
            {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "authenticator": {
                        "class_name": "source_greenhouse.components.NewAuthenticator"
                    },
                },
            },
            {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "authenticator": {
                        "type": "CustomAuthenticator",
                        "class_name": "source_greenhouse.components.NewAuthenticator",
                    },
                },
            },
            id="test_custom_authenticator",
        ),
        pytest.param(
            {
                "type": "SimpleRetriever",
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"class_name": "source_greenhouse.components.NewRecordExtractor"},
                },
            },
            {
                "type": "SimpleRetriever",
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "CustomRecordExtractor",
                        "class_name": "source_greenhouse.components.NewRecordExtractor",
                    },
                },
            },
            id="test_custom_extractor",
        ),
    ],
)
def test_transform_custom_components(component, expected_component):
    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


def test_propagate_parameters_to_all_components():
    component = {
        "type": "DeclarativeSource",
        "streams": [
            {
                "type": "DeclarativeStream",
                "$parameters": {"name": "roasters", "primary_key": "id"},
                "retriever": {
                    "type": "SimpleRetriever",
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "requester": {
                        "type": "HttpRequester",
                        "name": '{{ parameters["name"] }}',
                        "url_base": "https://coffee.example.io/v1/",
                        "http_method": "GET",
                    },
                },
            }
        ],
    }

    expected_component = {
        "type": "DeclarativeSource",
        "streams": [
            {
                "type": "DeclarativeStream",
                "retriever": {
                    "type": "SimpleRetriever",
                    "name": "roasters",
                    "primary_key": "id",
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {
                            "type": "DpathExtractor",
                            "field_path": [],
                            "name": "roasters",
                            "primary_key": "id",
                            "$parameters": {"name": "roasters", "primary_key": "id"},
                        },
                        "name": "roasters",
                        "primary_key": "id",
                        "$parameters": {"name": "roasters", "primary_key": "id"},
                    },
                    "requester": {
                        "type": "HttpRequester",
                        "name": '{{ parameters["name"] }}',
                        "url_base": "https://coffee.example.io/v1/",
                        "http_method": "GET",
                        "primary_key": "id",
                        "$parameters": {"name": "roasters", "primary_key": "id"},
                    },
                    "$parameters": {"name": "roasters", "primary_key": "id"},
                },
                "name": "roasters",
                "primary_key": "id",
                "$parameters": {"name": "roasters", "primary_key": "id"},
            }
        ],
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


def test_component_parameters_take_precedence_over_parent_parameters():
    component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "name": "high_priority",
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": "high_priority",
                },
            },
            "$parameters": {
                "name": "low_priority",
            },
        },
    }

    expected_component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "name": "low_priority",
            "requester": {
                "type": "HttpRequester",
                "name": "high_priority",
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": "high_priority",
                },
            },
            "$parameters": {
                "name": "low_priority",
            },
        },
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


def test_do_not_propagate_parameters_that_have_the_same_field_name():
    component = {
        "type": "DeclarativeStream",
        "streams": [
            {
                "type": "DeclarativeStream",
                "$parameters": {
                    "name": "roasters",
                    "primary_key": "id",
                    "schema_loader": {
                        "type": "JsonFileSchemaLoader",
                        "file_path": './source_coffee/schemas/{{ parameters["name"] }}.json',
                    },
                },
            }
        ],
    }

    expected_component = {
        "type": "DeclarativeStream",
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "roasters",
                "primary_key": "id",
                "schema_loader": {
                    "type": "JsonFileSchemaLoader",
                    "file_path": './source_coffee/schemas/{{ parameters["name"] }}.json',
                    "name": "roasters",
                    "primary_key": "id",
                    "$parameters": {
                        "name": "roasters",
                        "primary_key": "id",
                    },
                },
                "$parameters": {
                    "name": "roasters",
                    "primary_key": "id",
                    "schema_loader": {
                        "type": "JsonFileSchemaLoader",
                        "file_path": './source_coffee/schemas/{{ parameters["name"] }}.json',
                    },
                },
            }
        ],
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


def test_ignore_empty_parameters():
    component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
        },
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == component


def test_only_propagate_parameters_to_components():
    component = {
        "type": "ParentComponent",
        "component_with_object_properties": {
            "type": "TestComponent",
            "subcomponent": {
                "type": "TestSubComponent",
                "some_field": "high_priority",
                "$parameters": {
                    "some_option": "already",
                },
            },
            "dictionary_field": {
                "details": "should_not_contain_parameters",
                "other": "no_parameters_as_fields",
            },
            "$parameters": {
                "included": "not!",
            },
        },
    }

    expected_component = {
        "type": "ParentComponent",
        "component_with_object_properties": {
            "type": "TestComponent",
            "subcomponent": {
                "type": "TestSubComponent",
                "some_field": "high_priority",
                "some_option": "already",
                "included": "not!",
                "$parameters": {"some_option": "already", "included": "not!"},
            },
            "dictionary_field": {
                "details": "should_not_contain_parameters",
                "other": "no_parameters_as_fields",
            },
            "included": "not!",
            "$parameters": {
                "included": "not!",
            },
        },
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(
            {
                "type": "object",
                "$schema": "http://json-schema.org/schema#",
                "properties": {"id": {"type": "string"}},
            },
            id="object",
        ),
        pytest.param(
            {"type": ["null", "object"], "properties": {"id": {"type": ["null", "string"]}}},
            id="null_object",
        ),
        pytest.param(
            {"type": ["object", "null"], "properties": {"id": {"type": ["null", "string"]}}},
            id="object_null",
        ),
        pytest.param(
            {"type": ["object"], "properties": {"id": {"type": ["integer"]}}},
            id="object_only",
        ),
        pytest.param(
            {"properties": {"type": {"type": "string"}, "id": {"type": "string"}}},
            id="no_root_type_with_type_property",
        ),
    ],
)
def test_do_not_propagate_parameters_on_json_schema_object(schema):
    component = {
        "type": "DeclarativeStream",
        "streams": [
            {
                "type": "DeclarativeStream",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": schema,
                },
                "$parameters": {
                    "name": "roasters",
                    "primary_key": "id",
                },
            }
        ],
    }

    expected_component = {
        "type": "DeclarativeStream",
        "streams": [
            {
                "type": "DeclarativeStream",
                "name": "roasters",
                "primary_key": "id",
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "name": "roasters",
                    "primary_key": "id",
                    "schema": schema,
                    "$parameters": {
                        "name": "roasters",
                        "primary_key": "id",
                    },
                },
                "$parameters": {
                    "name": "roasters",
                    "primary_key": "id",
                },
            }
        ],
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})

    assert actual_component == expected_component


def test_propagate_property_chunking():
    component = {
        "type": "DeclarativeStream",
        "streams": [
            {
                "type": "DeclarativeStream",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://test.com",
                        "request_parameters": {
                            "properties": {
                                "type": "QueryProperties",
                                "property_list": {
                                    "type": "PropertiesFromEndpoint",
                                    "property_field_path": ["name"],
                                    "retriever": {
                                        "type": "SimpleRetriever",
                                        "requester": {
                                            "type": "HttpRequester",
                                            "url_base": "https://test.com",
                                            "authenticator": {
                                                "$ref": "#/definitions/authenticator"
                                            },
                                            "path": "/properties/{{ parameters.entity }}/properties",
                                            "http_method": "GET",
                                            "request_headers": {"Content-Type": "application/json"},
                                        },
                                    },
                                },
                                "property_chunking": {
                                    "type": "PropertyChunking",
                                    "property_limit_type": "characters",
                                    "property_limit": 15000,
                                },
                            }
                        },
                    },
                },
                "$parameters": {"entity": "test_entity"},
            }
        ],
    }
    expected_component = {
        "streams": [
            {
                "$parameters": {"entity": "test_entity"},
                "entity": "test_entity",
                "retriever": {
                    "$parameters": {"entity": "test_entity"},
                    "entity": "test_entity",
                    "requester": {
                        "$parameters": {"entity": "test_entity"},
                        "entity": "test_entity",
                        "request_parameters": {
                            "properties": {
                                "$parameters": {"entity": "test_entity"},
                                "entity": "test_entity",
                                "property_chunking": {
                                    "$parameters": {"entity": "test_entity"},
                                    "entity": "test_entity",
                                    "property_limit": 15000,
                                    "property_limit_type": "characters",
                                    "type": "PropertyChunking",
                                },
                                "property_list": {
                                    "$parameters": {"entity": "test_entity"},
                                    "entity": "test_entity",
                                    "property_field_path": ["name"],
                                    "retriever": {
                                        "$parameters": {"entity": "test_entity"},
                                        "entity": "test_entity",
                                        "requester": {
                                            "$parameters": {"entity": "test_entity"},
                                            "authenticator": {
                                                "$ref": "#/definitions/authenticator"
                                            },
                                            "entity": "test_entity",
                                            "http_method": "GET",
                                            "path": "/properties/{{ "
                                            "parameters.entity "
                                            "}}/properties",
                                            "request_headers": {"Content-Type": "application/json"},
                                            "type": "HttpRequester",
                                            "url_base": "https://test.com",
                                        },
                                        "type": "SimpleRetriever",
                                    },
                                    "type": "PropertiesFromEndpoint",
                                },
                                "type": "QueryProperties",
                            }
                        },
                        "type": "HttpRequester",
                        "url_base": "https://test.com",
                    },
                    "type": "SimpleRetriever",
                },
                "type": "DeclarativeStream",
            }
        ],
        "type": "DeclarativeStream",
    }
    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters("", component, {})
    assert actual_component == expected_component


@pytest.mark.parametrize(
    "use_parent_parameters, expected_retriever_name, expected_requester_name, expected_requester_params_name",
    [
        pytest.param(
            True,
            "parent_priority",
            "component_priority",
            "parent_priority",
            id="use_parent_parameters_true",
        ),
        pytest.param(
            False,
            "parent_priority",
            "component_priority",
            "component_priority",
            id="use_parent_parameters_false",
        ),
    ],
)
def test_use_parent_parameters_configuration(
    use_parent_parameters,
    expected_retriever_name,
    expected_requester_name,
    expected_requester_params_name,
):
    """Test that use_parent_parameters configuration controls parameter precedence."""
    component_with_parent_priority = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "name": "component_priority",
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": "component_priority",
                },
            },
            "$parameters": {
                "name": "parent_priority",
            },
        },
    }

    expected_component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "name": expected_retriever_name,
            "requester": {
                "type": "HttpRequester",
                "name": expected_requester_name,
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": expected_requester_params_name,
                },
            },
            "$parameters": {
                "name": "parent_priority",
            },
        },
    }

    transformer = ManifestComponentTransformer()
    actual_component = transformer.propagate_types_and_parameters(
        "", component_with_parent_priority, {}, use_parent_parameters=use_parent_parameters
    )
    assert actual_component == expected_component


def test_use_parent_parameters_none_behavior():
    """Test that use_parent_parameters=None maintains backward compatibility."""
    component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "name": "component_priority",
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": "component_priority",
                },
            },
            "$parameters": {
                "name": "parent_priority",
            },
        },
    }

    expected_component_priority = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "name": "parent_priority",  # Parent parameter takes precedence (default behavior)
            "requester": {
                "type": "HttpRequester",
                "name": "component_priority",  # Component parameter takes precedence
                "url_base": "https://coffee.example.io/v1/",
                "http_method": "GET",
                "primary_key": "id",
                "$parameters": {
                    "name": "component_priority",
                },
            },
            "$parameters": {
                "name": "parent_priority",
            },
        },
    }

    transformer = ManifestComponentTransformer()
    actual = transformer.propagate_types_and_parameters(
        "", component, {}, use_parent_parameters=None
    )
    assert actual == expected_component_priority


def test_dynamic_stream_use_parent_parameters_configuration():
    """Test that use_parent_parameters configuration is properly read from dynamic stream definitions."""

    transformer = ManifestComponentTransformer()

    # Only parent has $parameters
    component = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
        },
        "$parameters": {"name": "parent_name"},
    }

    # When use_parent_parameters=False, component parameters should take precedence (but there are none)
    result_false = transformer.propagate_types_and_parameters(
        "", component, {}, use_parent_parameters=False
    )
    # When use_parent_parameters=True, parent parameters should take precedence (and are used)
    result_true = transformer.propagate_types_and_parameters(
        "", component, {}, use_parent_parameters=True
    )

    # In both cases, since only the parent has $parameters, the retriever should get "parent_name"
    assert result_false["retriever"]["name"] == "parent_name"
    assert result_true["retriever"]["name"] == "parent_name"

    # Now, add a $parameters to the retriever to see the difference
    component_with_both = {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "$parameters": {"name": "retriever_name"},
        },
        "$parameters": {"name": "parent_name"},
    }

    result_false = transformer.propagate_types_and_parameters(
        "", component_with_both, {}, use_parent_parameters=False
    )
    result_true = transformer.propagate_types_and_parameters(
        "", component_with_both, {}, use_parent_parameters=True
    )

    # When use_parent_parameters=False, retriever's own $parameters win
    assert result_false["retriever"]["name"] == "retriever_name"
    # When use_parent_parameters=True, parent's $parameters win
    assert result_true["retriever"]["name"] == "parent_name"


# --- `STREAM_INDEPENDENT_FIELDS`: outer parameters do not fill a credential login ------------

_STREAM_PARAMETERS = {
    "name": "users",
    "path": "/users",
    "url": "https://api.example.com/users",
    "use_cache": True,
    "max_retries": 9,
    "action": "IGNORE",
    "field_name": "stream_field",
}


def _stream_with_login(login_requester_updates=None, login_updates=None):
    login_requester = {
        "type": "HttpRequester",
        "url_base": "https://api.example.com",
        "path": "/login",
        **(login_requester_updates or {}),
    }
    return {
        "type": "DeclarativeStream",
        "retriever": {
            "type": "SimpleRetriever",
            "requester": {
                "type": "HttpRequester",
                "url_base": "https://api.example.com",
                "authenticator": {
                    "type": "RateLimitedMultipleTokenAuthenticator",
                    "credentials": {
                        "type": "CredentialSet",
                        "items": "{{ config['accounts'] }}",
                        "authenticator": {
                            "type": "SessionTokenAuthenticator",
                            "login_requester": login_requester,
                            **(login_updates or {}),
                        },
                    },
                },
            },
            "record_selector": {
                "type": "RecordSelector",
                "extractor": {"type": "DpathExtractor", "field_path": []},
            },
        },
        "$parameters": dict(_STREAM_PARAMETERS),
    }


def _login(propagated):
    authenticator = propagated["retriever"]["requester"]["authenticator"]
    return authenticator["credentials"]["authenticator"]


def _propagate_like_a_dynamic_stream(stream):
    """Propagate twice, as a resolved dynamic stream is: once with the manifest, once on its own."""
    transformer = ManifestComponentTransformer()
    first = transformer.propagate_types_and_parameters("", stream, {})
    return transformer.propagate_types_and_parameters("", first, {}, use_parent_parameters=True)


@pytest.mark.parametrize(
    "propagate",
    [
        pytest.param(
            lambda stream: ManifestComponentTransformer().propagate_types_and_parameters(
                "", stream, {}
            ),
            id="once",
        ),
        pytest.param(_propagate_like_a_dynamic_stream, id="twice_like_a_dynamic_stream"),
    ],
)
def test_stream_parameters_fill_no_field_inside_a_credential_login(propagate):
    stream = _stream_with_login(
        login_requester_updates={
            # A list of typed components.
            "error_handler": {
                "type": "CompositeErrorHandler",
                "error_handlers": [
                    {
                        "type": "DefaultErrorHandler",
                        "response_filters": [{"type": "HttpResponseFilter", "http_codes": [401]}],
                    }
                ],
            },
            # An untyped object holding a typed component.
            "options": {"token": {"type": "RequestOption", "inject_into": "header"}},
        }
    )

    propagated = propagate(stream)

    login = _login(propagated)
    login_requester = login["login_requester"]
    assert login_requester["path"] == "/login"
    for field in ("url", "use_cache", "name"):
        assert field not in login_requester
    error_handler = login_requester["error_handler"]["error_handlers"][0]
    assert "max_retries" not in error_handler
    assert "action" not in error_handler["response_filters"][0]
    assert "field_name" not in login_requester["options"]["token"]
    # The stream's parameters can still be read as `parameters`.
    assert login_requester["$parameters"]["url"] == _STREAM_PARAMETERS["url"]
    assert error_handler["$parameters"]["max_retries"] == 9


@pytest.mark.parametrize(
    "propagate",
    [
        pytest.param(
            lambda stream: ManifestComponentTransformer().propagate_types_and_parameters(
                "", stream, {}
            ),
            id="once",
        ),
        pytest.param(_propagate_like_a_dynamic_stream, id="twice_like_a_dynamic_stream"),
    ],
)
def test_parameters_declared_inside_a_credential_login_fill_its_fields(propagate):
    stream = _stream_with_login(
        login_requester_updates={"path": None, "$parameters": {"path": "/sessions"}},
        login_updates={"$parameters": {"use_cache": True}},
    )
    del stream["retriever"]["requester"]["authenticator"]["credentials"]["authenticator"][
        "login_requester"
    ]["path"]

    propagated = propagate(stream)

    login_requester = _login(propagated)["login_requester"]
    assert login_requester["path"] == "/sessions"
    assert login_requester["use_cache"] is True
    assert "url" not in login_requester


def test_a_credential_login_parameter_named_like_a_field_is_not_propagated_into_it():
    stream = _stream_with_login(login_updates={"$parameters": {"login_requester": "not a field"}})

    propagated = ManifestComponentTransformer().propagate_types_and_parameters("", stream, {})

    assert "login_requester" not in _login(propagated)["login_requester"]


def test_components_around_a_credential_login_are_still_filled_with_stream_parameters():
    propagated = ManifestComponentTransformer().propagate_types_and_parameters(
        "", _stream_with_login(), {}
    )

    requester = propagated["retriever"]["requester"]
    assert requester["path"] == "/users"
    assert requester["authenticator"]["name"] == "users"
    # A sibling processed after the requester that holds the login.
    assert propagated["retriever"]["record_selector"]["name"] == "users"
    assert propagated["retriever"]["record_selector"]["extractor"]["name"] == "users"
