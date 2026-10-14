# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the structural, presentational and OAuth spec lints in `_spec_lint`."""

import warnings
from typing import Any, Callable, Mapping

import pytest

from airbyte_cdk.models import ConnectorSpecification
from airbyte_cdk.test.standard_tests import SpecLintWarning
from airbyte_cdk.test.standard_tests._spec_lint import (
    SPEC_LINTS,
    SpecLint,
    assert_spec_is_valid,
    find_additional_properties_errors,
    find_array_type_errors,
    find_display_type_errors,
    find_duplicate_order_errors,
    find_empty_object_errors,
    find_enum_usage_errors,
    find_forbidden_keyword_errors,
    find_invalid_schema_errors,
    find_nested_group_errors,
    find_oauth_parameter_errors,
    find_oneof_usage_errors,
    find_property_type_errors,
    find_spec_problems,
    find_unresolved_ref_errors,
    format_spec_problems,
)

Lint = Callable[[Mapping[str, Any]], list[str]]


def _object(properties: dict[str, Any], **keywords: Any) -> dict[str, Any]:
    return {"type": "object", "properties": properties, **keywords}


def _variant(
    discriminator: dict[str, Any], name: str = "auth_type", **properties: Any
) -> dict[str, Any]:
    return _object({name: discriminator, **properties})


def _with_oneof(*variants: Any, **keywords: Any) -> dict[str, Any]:
    return _object({"credentials": {"type": "object", "oneOf": list(variants), **keywords}})


def _assert_errors(lint: Lint, schema: Mapping[str, Any], expected: list[str]) -> None:
    """Assert one error per expected pointer, each naming its pointer, in order."""
    errors = lint(schema)
    assert len(errors) == len(expected), errors
    for error, pointer in zip(errors, expected):
        assert error.startswith(f"`{pointer}`"), (error, pointer)


OAUTH_TYPE = {"type": "string", "const": "oauth"}
TOKEN_TYPE = {"type": "string", "const": "token"}


@pytest.mark.parametrize(
    "schema, expected",
    [
        pytest.param(_with_oneof(_variant(OAUTH_TYPE), _variant(TOKEN_TYPE)), [], id="const"),
        pytest.param(
            _with_oneof(
                _variant({**OAUTH_TYPE, "default": "oauth", "enum": ["oauth"]}),
                _variant(TOKEN_TYPE),
            ),
            [],
            id="const_with_matching_default_and_enum",
        ),
        pytest.param(
            _with_oneof(
                _variant({"type": "string", "enum": ["oauth"], "default": "oauth"}),
                _variant({"type": "string", "enum": ["token"], "default": "token"}),
            ),
            [],
            id="single_item_enum_with_default",
        ),
        pytest.param(
            _with_oneof(
                _variant(OAUTH_TYPE, codec={"type": "string", "enum": ["gzip", "none"]}),
                _variant(TOKEN_TYPE, codec={"type": "string", "enum": ["gzip", "none"]}),
            ),
            [],
            id="shared_multi_item_enum_is_not_a_candidate",
        ),
        pytest.param(
            _with_oneof(
                _variant(OAUTH_TYPE, mode={"type": "string", "enum": ["a"], "default": "a"}),
                _variant(TOKEN_TYPE, mode={"type": "string", "enum": ["b"], "default": "b"}),
            ),
            [],
            id="one_const_candidate_wins_over_enum_candidates",
        ),
        pytest.param(
            _object({"credentials": {"oneOf": [_variant(OAUTH_TYPE), _variant(TOKEN_TYPE)]}}),
            ["/properties/credentials/oneOf"],
            id="not_an_object",
        ),
        pytest.param(
            _with_oneof(_variant(OAUTH_TYPE), {"type": "object"}),
            ["/properties/credentials/oneOf/1"],
            id="variant_without_properties",
        ),
        pytest.param(
            _with_oneof(_variant(OAUTH_TYPE), _variant(TOKEN_TYPE, name="method")),
            ["/properties/credentials/oneOf"],
            id="no_shared_discriminator",
        ),
        pytest.param(
            _with_oneof(
                _variant(OAUTH_TYPE, kind={"const": "a"}),
                _variant(TOKEN_TYPE, kind={"const": "b"}),
            ),
            ["/properties/credentials/oneOf"],
            id="two_const_discriminators",
        ),
        pytest.param(
            _with_oneof(
                _variant({"type": "string", "enum": ["oauth"], "default": "oauth"}),
                _variant({"type": "string", "enum": ["token"], "default": "token"}),
                _variant({"type": "string", "enum": ["key"], "default": "key"}, name="x"),
            ),
            ["/properties/credentials/oneOf"],
            id="discriminator_missing_from_a_variant",
        ),
        pytest.param(
            _with_oneof(_variant({**OAUTH_TYPE, "default": "token"}), _variant(TOKEN_TYPE)),
            ["/properties/credentials/oneOf/0/properties/auth_type"],
            id="default_differs_from_const",
        ),
        pytest.param(
            _with_oneof(_variant({**OAUTH_TYPE, "enum": ["token"]}), _variant(TOKEN_TYPE)),
            ["/properties/credentials/oneOf/0/properties/auth_type"],
            id="enum_differs_from_const",
        ),
        pytest.param(
            _with_oneof(
                _variant({"type": "string", "enum": ["oauth"]}),
                _variant({"type": "string", "enum": ["token"], "default": "token"}),
            ),
            ["/properties/credentials/oneOf/0/properties/auth_type"],
            id="enum_discriminator_without_default",
        ),
        pytest.param(
            _with_oneof(_variant(OAUTH_TYPE), _variant(OAUTH_TYPE)),
            ["/properties/credentials/oneOf"],
            id="duplicate_discriminator_value",
        ),
    ],
)
def test_find_oneof_usage_errors(schema: dict[str, Any], expected: list[str]) -> None:
    _assert_errors(find_oneof_usage_errors, schema, expected)


@pytest.mark.parametrize(
    "lint, schema, expected",
    [
        # enum usage
        pytest.param(
            find_enum_usage_errors,
            _object({"a": {"enum": ["x", "y", 1, {"k": 1}]}}),
            [],
            id="enum_ok",
        ),
        pytest.param(
            find_enum_usage_errors,
            _object({"a": {"enum": ["x", "y", "x", "x"]}}),
            ["/properties/a/enum", "/properties/a/enum"],
            id="enum_duplicates",
        ),
        # forbidden keywords
        pytest.param(
            find_forbidden_keyword_errors,
            _object({"not": {"type": "string"}, "if": {"type": "string"}}),
            [],
            id="forbidden_keyword_as_property_name",
        ),
        pytest.param(
            find_forbidden_keyword_errors,
            _object(
                {
                    "a": {"allOf": [{"type": "string"}]},
                    "b": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                },
                patternProperties={"^x": {"type": "string"}},
            ),
            ["/patternProperties", "/properties/a/allOf", "/properties/b/anyOf"],
            id="forbidden_keywords",
        ),
        # array items
        pytest.param(
            find_array_type_errors,
            _object(
                {
                    "a": {"type": "array", "items": {"type": "string"}},
                    "b": {"type": "array", "items": {"enum": ["x", "y"]}},
                    "c": {"type": "array", "items": _object({"x": {"type": "string"}})},
                    "d": {"type": "array"},
                }
            ),
            [],
            id="array_ok",
        ),
        pytest.param(
            find_array_type_errors,
            _object(
                {
                    "a": {"type": "array", "items": [{"type": "string"}]},
                    "b": {"type": "array", "items": {"type": "boolean"}},
                    "c": {"type": "array", "items": {"type": "array"}},
                }
            ),
            ["/properties/a/items", "/properties/b/items", "/properties/c/items"],
            id="array_bad_items",
        ),
        # object not empty
        pytest.param(find_empty_object_errors, {"type": "object"}, [], id="empty_root_ok"),
        pytest.param(
            find_empty_object_errors,
            _object(
                {
                    "a": _object({"x": {"type": "string"}}),
                    "b": {"type": "object", "oneOf": [_object({"x": {"const": "y"}})]},
                }
            ),
            [],
            id="objects_ok",
        ),
        pytest.param(
            find_empty_object_errors,
            _object(
                {
                    "a": {"type": "object"},
                    "b": {"type": "object", "properties": {}},
                    "c": {"type": "array", "items": {"type": "object"}},
                }
            ),
            ["/properties/a", "/properties/b", "/properties/c/items"],
            id="empty_objects",
        ),
        # property types
        pytest.param(
            find_property_type_errors,
            _object(
                {
                    "a": {"type": "string"},
                    "b": {"type": ["string"]},
                    "c": {"type": ["integer", "null"]},
                }
            ),
            [],
            id="property_types_ok",
        ),
        pytest.param(
            find_property_type_errors,
            _object(
                {
                    "a": {"type": ["null", "string"]},
                    "b": {"type": ["string", "integer"]},
                    "c": {"type": ["string", "integer", "null"]},
                }
            ),
            ["/properties/a/type", "/properties/b/type", "/properties/c/type"],
            id="property_types_bad",
        ),
        # additionalProperties
        pytest.param(
            find_additional_properties_errors,
            _object({"a": _object({}, additionalProperties=True)}, additionalProperties=True),
            [],
            id="additional_properties_true",
        ),
        pytest.param(
            find_additional_properties_errors,
            _object(
                {"a": _object({}, additionalProperties={"type": "string"})},
                additionalProperties=False,
            ),
            ["/additionalProperties", "/properties/a/additionalProperties"],
            id="additional_properties_not_true",
        ),
        # display_type
        pytest.param(
            find_display_type_errors,
            _with_oneof(_variant(OAUTH_TYPE), display_type="radio"),
            [],
            id="display_type_ok",
        ),
        pytest.param(
            find_display_type_errors,
            _object(
                {
                    "a": {"type": "string", "display_type": "dropdown"},
                    "b": {"type": "object", "oneOf": [], "display_type": "list"},
                }
            ),
            ["/properties/a/display_type", "/properties/b/display_type"],
            id="display_type_bad",
        ),
        # duplicate order
        pytest.param(
            find_duplicate_order_errors,
            _object(
                {
                    "a": {"type": "string", "order": 0},
                    "b": {"type": "string", "order": 1},
                    "c": {"type": "string", "order": 0, "group": "advanced"},
                    "d": _object(
                        {"x": {"type": "string", "order": 0}, "y": {"type": "string", "order": 1}}
                    ),
                }
            ),
            [],
            id="order_ok",
        ),
        pytest.param(
            find_duplicate_order_errors,
            _object(
                {
                    "a": {"type": "string", "order": 0},
                    "b": {"type": "string", "order": 0},
                    "c": {"type": "string", "order": 1, "group": "g"},
                    "d": {"type": "string", "order": 1, "group": "g"},
                }
            ),
            ["/properties/b/order", "/properties/d/order"],
            id="order_duplicates",
        ),
        # nested group
        pytest.param(
            find_nested_group_errors,
            _object({"a": {"type": "string", "group": "auth"}}),
            [],
            id="group_top_level",
        ),
        pytest.param(
            find_nested_group_errors,
            _object({"a": _object({"b": {"type": "string", "group": "auth"}})}),
            ["/properties/a/properties/b/group"],
            id="group_nested",
        ),
        # valid JSON Schema
        pytest.param(
            find_invalid_schema_errors,
            _object({"a": {"type": "string", "minLength": 1}}, required=["a"]),
            [],
            id="valid_schema",
        ),
        pytest.param(
            find_invalid_schema_errors,
            _object({"a": {"type": "text"}}, required="a"),
            ["/properties/a/type", "/required"],
            id="invalid_schema",
        ),
    ],
)
def test_structural_lints(lint: Lint, schema: dict[str, Any], expected: list[str]) -> None:
    errors = lint(schema)
    assert len(errors) == len(expected), errors
    for error, pointer in zip(sorted(errors), sorted(expected)):
        assert error.startswith(f"`{pointer}"), (error, pointer)


def test_nested_group_error_names_the_group_keyword() -> None:
    errors = find_nested_group_errors(_object({"a": _object({"b": {"group": "g"}})}))
    assert errors == [
        "`/properties/a/properties/b/group` is set below the top level; only top-level "
        "properties can be grouped."
    ]


def test_lints_ignore_values_that_look_like_schemas() -> None:
    schema = _object(
        {"a": {"type": "string", "default": {"type": "object"}, "examples": [{"allOf": []}]}}
    )
    for lint in (find_empty_object_errors, find_forbidden_keyword_errors):
        assert lint(schema) == []


@pytest.mark.parametrize(
    "spec, expected",
    [
        pytest.param({"connectionSpecification": _object({"a": {"type": "string"}})}, [], id="ok"),
        pytest.param(
            {
                "connectionSpecification": _object({"a": {"$ref": "#/definitions/a"}}),
                "advanced_auth": {"oauth_config_specification": {"x": {"$ref": "#/y"}}},
            },
            [
                "/connectionSpecification/properties/a/$ref",
                "/advanced_auth/oauth_config_specification/x/$ref",
            ],
            id="refs",
        ),
    ],
)
def test_find_unresolved_ref_errors(spec: dict[str, Any], expected: list[str]) -> None:
    _assert_errors(find_unresolved_ref_errors, spec, expected)


def _oauth_spec(predicate_key: list[str], output_path: list[str]) -> dict[str, Any]:
    return {
        "connectionSpecification": _object(
            {
                "credentials": {
                    "type": "object",
                    "oneOf": [
                        _variant(
                            OAUTH_TYPE,
                            client_id={"type": "string", "airbyte_secret": True},
                            refresh_token={"type": "string", "airbyte_secret": True},
                        ),
                        _variant(TOKEN_TYPE, api_token={"type": "string", "airbyte_secret": True}),
                    ],
                },
                "accounts": {
                    "type": "array",
                    "items": _object({"id": {"type": "string"}}),
                },
            }
        ),
        "advanced_auth": {
            "auth_flow_type": "oauth2.0",
            "predicate_key": predicate_key,
            "predicate_value": "oauth",
            "oauth_config_specification": {
                "complete_oauth_output_specification": {
                    "type": "object",
                    "properties": {
                        "refresh_token": {
                            "type": "string",
                            "path_in_connector_config": output_path,
                        }
                    },
                },
                "complete_oauth_server_output_specification": {
                    "type": "object",
                    "properties": {
                        "client_id": {
                            "type": "string",
                            "path_in_connector_config": ["credentials", "client_id"],
                        },
                        "account": {
                            "type": "string",
                            "path_in_connector_config": ["accounts", "[]", "id"],
                        },
                    },
                },
            },
        },
    }


def test_find_oauth_parameter_errors_accepts_paths_through_oneof_and_arrays() -> None:
    spec = _oauth_spec(["credentials", "auth_type"], ["credentials", "refresh_token"])
    assert find_oauth_parameter_errors(spec) == []


def test_find_oauth_parameter_errors_reports_each_missing_path() -> None:
    spec = _oauth_spec(["credentials", "method"], ["refresh_token"])
    assert find_oauth_parameter_errors(spec) == [
        "`/advanced_auth/predicate_key` names the config path `/credentials/method`, which is "
        "not a property of `connectionSpecification`.",
        "`/advanced_auth/oauth_config_specification/complete_oauth_output_specification/"
        "properties/refresh_token/path_in_connector_config` names the config path "
        "`/refresh_token`, which is not a property of `connectionSpecification`.",
    ]


def test_find_oauth_parameter_errors_without_advanced_auth() -> None:
    assert find_oauth_parameter_errors({"connectionSpecification": _object({})}) == []


def test_pointer_escapes_slash_and_tilde() -> None:
    errors = find_additional_properties_errors(
        _object({"a/b": _object({}, additionalProperties=False), "c~d": {"type": "string"}})
    )
    assert errors[0].startswith("`/properties/a~1b/additionalProperties`")


def test_spec_lints_have_unique_names() -> None:
    names = [lint.name for lint in SPEC_LINTS]
    assert len(names) == len(set(names))


def test_advisory_lints() -> None:
    assert {lint.name for lint in SPEC_LINTS if not lint.enforced} == {
        "forbidden keywords",
        "object not empty",
    }


def test_find_spec_problems_reports_a_crashing_lint_without_raising() -> None:
    def crash(spec: Mapping[str, Any]) -> list[str]:
        raise KeyError("sk_live_should_not_be_printed")

    lint = SpecLint("crash", "Never crashes.", crash, enforced=False)
    problems = find_spec_problems({"connectionSpecification": _object({})}, lints=[lint])
    assert problems == {lint: ["The lint could not run: KeyError."]}


def test_find_spec_problems_on_a_clean_spec() -> None:
    spec = {
        "connectionSpecification": _object(
            {"api_key": {"type": "string", "airbyte_secret": True, "order": 0}},
            additionalProperties=True,
        )
    }
    assert find_spec_problems(spec) == {}


def test_format_spec_problems_groups_by_lint() -> None:
    first = SpecLint("first", "Rule one.", lambda spec: [])
    second = SpecLint("second", "Rule two.", lambda spec: [])
    text = format_spec_problems({first: ["a", "b"], second: ["c"]})
    assert text == "[first] Rule one.\n  - a\n  - b\n[second] Rule two.\n  - c"


def _connector_spec(properties: dict[str, Any], **keywords: Any) -> ConnectorSpecification:
    return ConnectorSpecification(connectionSpecification=_object(properties, **keywords))


def test_assert_spec_is_valid_fails_on_an_enforced_lint_and_names_it() -> None:
    spec = _connector_spec({"a": {"type": "string"}}, additionalProperties=False)
    with pytest.raises(AssertionError) as error:
        assert_spec_is_valid(spec, connector_name="source-test")
    message = str(error.value)
    assert "[additionalProperties is true]" in message
    assert "`/additionalProperties`" in message
    assert "connector-specification-reference" in message


def test_assert_spec_is_valid_warns_on_an_advisory_lint() -> None:
    spec = _connector_spec({"a": {"type": "object"}, "b": {"allOf": [{"type": "string"}]}})
    with pytest.warns(SpecLintWarning) as record:
        assert_spec_is_valid(spec, connector_name="source-test")
    assert len(record) == 1
    message = str(record[0].message)
    assert "[object not empty]" in message and "`/properties/a`" in message
    assert "[forbidden keywords]" in message and "`/properties/b/allOf`" in message


def test_assert_spec_is_valid_warns_before_it_fails() -> None:
    spec = _connector_spec({"a": {"type": "object"}}, additionalProperties=False)
    with pytest.warns(SpecLintWarning), pytest.raises(AssertionError) as error:
        assert_spec_is_valid(spec, connector_name="source-test")
    assert "object not empty" not in str(error.value)


def test_assert_spec_is_valid_is_silent_on_a_clean_spec() -> None:
    spec = _connector_spec({"a": {"type": "string", "order": 0}})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert_spec_is_valid(spec, connector_name="source-test")


def test_failure_messages_do_not_print_values() -> None:
    marker = "sk_live_value_from_the_spec"
    spec = _connector_spec(
        {
            "a": {"type": "string", "enum": [marker, marker], "display_type": marker},
            "b": {"type": "string", "order": 3, "default": marker},
            "c": {"type": "string", "order": 3, "examples": [marker]},
        },
        additionalProperties={"description": marker},
    )
    with pytest.raises(AssertionError) as error:
        assert_spec_is_valid(spec, connector_name="source-test")
    assert marker not in str(error.value)
