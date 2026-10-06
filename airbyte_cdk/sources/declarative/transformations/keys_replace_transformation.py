#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import copy
import re
from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional, Pattern, Union

import dpath

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


def _is_interpolated(value: str) -> bool:
    return "{{" in value or "{%" in value


@dataclass
class KeysReplaceTransformation(RecordTransformation):
    """
    Transformation that applies keys names replacement.

    Example usage:
    - type: KeysReplace
      old: " "
      new: "_"
    Result:
    from: {"created time": ..., "customer id": ..., "user id": ...}
    to: {"created_time": ..., "customer_id": ..., "user_id": ...}

    By default, `old` is matched literally, matching keys are renamed and the replacement is applied
    recursively to nested objects of the whole record. Optional flags change this behavior:
    - regex: `old` is a regular expression and `new` a `re.sub` replacement (backreferences supported).
    - keep_original: the value is copied to the new key and the original key is kept.
    - only_if_missing: the new key is only written when it is absent or null.
    - field_path: the replacement is scoped to the object at this path (`*` matches every element).
    Keys are evaluated against a snapshot of each object, so keys written by the transformation are not
    processed again.
    """

    old: str
    new: str
    parameters: InitVar[Mapping[str, Any]]
    regex: bool = False
    keep_original: bool = False
    only_if_missing: bool = False
    field_path: Optional[List[Union[InterpolatedString, str]]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._old = InterpolatedString.create(self.old, parameters=parameters)
        self._new = InterpolatedString.create(self.new, parameters=parameters)
        self._field_path = [
            InterpolatedString.create(path, parameters=parameters)
            for path in (self.field_path or [])
        ]
        self._static_pattern: Optional[Pattern[str]] = None
        if self.regex and not _is_interpolated(self.old):
            self._static_pattern = self._compile_pattern(self.old)
            if not _is_interpolated(self.new):
                self._validate_replacement(self._static_pattern, self.new)

    @staticmethod
    def _compile_pattern(pattern: str) -> Pattern[str]:
        try:
            return re.compile(pattern)
        except re.error as exception:
            raise ValueError(
                f"KeysReplace `old` is not a valid regular expression: {pattern!r} ({exception})."
            ) from exception

    @staticmethod
    def _validate_replacement(pattern: Pattern[str], replacement: str) -> None:
        try:
            pattern.sub(replacement, "")
        except (re.error, IndexError) as exception:
            raise ValueError(
                f"KeysReplace `new` is not a valid replacement for pattern {pattern.pattern!r}: {replacement!r} ({exception})."
            ) from exception

    def _eval_regex_part(
        self, raw: str, interpolated: InterpolatedString, config: Config, **kwargs: Any
    ) -> str:
        # Plain strings are used as-is: Jinja evaluation runs `ast.literal_eval` on the output,
        # which would turn a pattern like `(1)` into `1`.
        if not _is_interpolated(raw):
            return raw
        return str(interpolated.eval(config, valid_types=(str,), **kwargs))

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        if config is None:
            config = {}

        kwargs = {"record": record, "stream_state": stream_state, "stream_slice": stream_slice}
        if self.regex:
            pattern = self._static_pattern or self._compile_pattern(
                self._eval_regex_part(self.old, self._old, config, **kwargs)
            )
            replacement = self._eval_regex_part(self.new, self._new, config, **kwargs)
            if self._static_pattern is None or _is_interpolated(self.new):
                self._validate_replacement(pattern, replacement)

            def replace_key(key: str) -> str:
                return pattern.sub(replacement, key)
        else:
            old_key = str(self._old.eval(config, **kwargs))
            new_key = str(self._new.eval(config, **kwargs))

            def replace_key(key: str) -> str:
                return key.replace(old_key, new_key)

        def _transform(data: Dict[str, Any]) -> Dict[str, Any]:
            transformed = {
                key: _transform(value) if isinstance(value, dict) else value
                for key, value in data.items()
            }

            if self.keep_original:
                result = dict(transformed)
                for key, value in transformed.items():
                    updated_key = replace_key(key)
                    if updated_key == key:
                        continue
                    if self.only_if_missing and result.get(updated_key) is not None:
                        continue
                    result[updated_key] = (
                        copy.deepcopy(value) if isinstance(value, (dict, list)) else value
                    )
                return result

            result = {}
            for key, value in transformed.items():
                updated_key = replace_key(key)
                if self.only_if_missing:
                    if updated_key != key and (
                        result.get(updated_key) is not None
                        or transformed.get(updated_key) is not None
                    ):
                        updated_key = key
                    if value is None and result.get(updated_key) is not None:
                        continue
                result[updated_key] = value
            return result

        for target in self._get_targets(record, config):
            transformed_target = _transform(target)
            target.clear()
            target.update(transformed_target)

    def _get_targets(self, record: Dict[str, Any], config: Config) -> List[Dict[str, Any]]:
        if not self._field_path:
            return [record]
        path = [str(path.eval(config)) for path in self._field_path]
        if "*" in path:
            matches = dpath.values(record, path)
        else:
            matches = [dpath.get(record, path, default=None)]
        return [match for match in matches if isinstance(match, dict)]
