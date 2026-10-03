#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from jsonschema.exceptions import ValidationError

_MAX_VALUE_REPR_LENGTH = 100
_ROOT_PATH = "<root>"


def format_manifest_validation_error(error: ValidationError) -> str:
    """Builds a user-facing message naming the failing manifest field and the violated constraint.

    `error` is expected to be the error raised by `jsonschema.validate`, i.e. the result of
    `jsonschema.exceptions.best_match`. For `anyOf`/`oneOf` errors, the sub-errors of the branches
    are merged so that e.g. a wrong `type` discriminator reports every allowed value instead of
    the opaque "is not valid under any of the given schemas".
    """
    path, constraint = _describe(error)
    return f"Manifest field '{path}' is invalid: {constraint}."


def _describe(error: ValidationError) -> Tuple[str, str]:
    base_path: List[Union[str, int]] = list(error.absolute_path)
    if error.validator in ("anyOf", "oneOf") and error.context:
        merged = _merge_branch_errors(error, base_path)
        if merged:
            return merged
        return (
            _format_path(base_path),
            "the value does not match any of the allowed component definitions",
        )
    return _format_path(base_path), _describe_single(error)


def _merge_branch_errors(
    error: ValidationError, base_path: List[Union[str, int]]
) -> Optional[Tuple[str, str]]:
    by_path: Dict[Tuple[Union[str, int], ...], List[ValidationError]] = defaultdict(list)
    for sub_error in error.context:
        by_path[tuple(sub_error.relative_path)].append(sub_error)
    shallowest = min(by_path, key=len)
    candidates = by_path[shallowest]
    validators = {sub_error.validator for sub_error in candidates}
    if validators == {"enum"}:
        allowed: List[Any] = []
        for sub_error in candidates:
            for value in sub_error.validator_value:
                if value not in allowed:
                    allowed.append(value)
        return (
            _format_path(base_path + list(shallowest)),
            f"value {_short_repr(candidates[0].instance)} is not one of {_format_options(allowed)}",
        )
    if validators == {"required"}:
        present = candidates[0].instance if isinstance(candidates[0].instance, dict) else {}
        missing: List[str] = []
        for sub_error in candidates:
            for prop in sub_error.validator_value:
                if prop not in present and prop not in missing:
                    missing.append(prop)
        return (
            _format_path(base_path + list(shallowest)),
            f"one of the following properties is required: {', '.join(repr(prop) for prop in missing)}",
        )
    return None


def _describe_single(error: ValidationError) -> str:
    if error.validator == "enum":
        return f"value {_short_repr(error.instance)} is not one of {_format_options(error.validator_value)}"
    if error.validator == "type":
        expected = error.validator_value
        expected_str = (
            " or ".join(f"'{t}'" for t in expected)
            if isinstance(expected, list)
            else f"'{expected}'"
        )
        return f"value {_short_repr(error.instance)} is not of type {expected_str}"
    return str(error.message)


def _format_path(path: Sequence[Union[str, int]]) -> str:
    if not path:
        return _ROOT_PATH
    formatted = ""
    for part in path:
        if isinstance(part, int):
            formatted += f"[{part}]"
        else:
            formatted += f".{part}" if formatted else str(part)
    return formatted


def _format_options(options: Sequence[Any]) -> str:
    return "[" + ", ".join(repr(option) for option in options) + "]"


def _short_repr(value: Any) -> str:
    text = repr(value)
    if len(text) > _MAX_VALUE_REPR_LENGTH:
        return text[: _MAX_VALUE_REPR_LENGTH - 3] + "..."
    return text
