# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import functools
import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airbyte_cdk.test.mock_http import HttpResponse
from airbyte_cdk.test.utils.data import get_unit_test_folder


if TYPE_CHECKING:
    from pathlib import Path as FilePath


def _extract(path: list[str], response_template: dict[str, Any]) -> Any:  # noqa: ANN401  (any-type)
    return functools.reduce(lambda a, b: a[b], path, response_template)


def _replace_value(dictionary: dict[str, Any], path: list[str], value: Any) -> None:  # noqa: ANN401  (any-type)
    current = dictionary
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = value


def _write(dictionary: dict[str, Any], path: list[str], value: Any) -> None:  # noqa: ANN401  (any-type)
    current = dictionary
    for key in path[:-1]:
        current = current.setdefault(key, {})
    current[path[-1]] = value


class Path(ABC):
    @abstractmethod
    def write(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        pass

    @abstractmethod
    def update(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        pass

    def extract(self, template: dict[str, Any]) -> Any:  # noqa: ANN401, B027  (any-type, intentionally empty)
        pass


class FieldPath(Path):
    def __init__(self, field: str) -> None:
        self._path = [field]

    def write(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        _write(template, self._path, value)

    def update(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        _replace_value(template, self._path, value)

    def extract(self, template: dict[str, Any]) -> Any:  # noqa: ANN401  (any-type)
        return _extract(self._path, template)

    def __str__(self) -> str:
        return f"FieldPath(field={self._path[0]})"


class NestedPath(Path):
    def __init__(self, path: list[str]) -> None:
        self._path = path

    def write(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        _write(template, self._path, value)

    def update(self, template: dict[str, Any], value: Any) -> None:  # noqa: ANN401  (any-type)
        _replace_value(template, self._path, value)

    def extract(self, template: dict[str, Any]) -> Any:  # noqa: ANN401  (any-type)
        return _extract(self._path, template)

    def __str__(self) -> str:
        return f"NestedPath(path={self._path})"


class PaginationStrategy(ABC):
    @abstractmethod
    def update(self, response: dict[str, Any]) -> None:
        pass


class FieldUpdatePaginationStrategy(PaginationStrategy):
    def __init__(self, path: Path, value: Any) -> None:  # noqa: ANN401  (any-type)
        self._path = path
        self._value = value

    def update(self, response: dict[str, Any]) -> None:
        self._path.update(response, self._value)


class RecordBuilder:
    def __init__(
        self,
        template: dict[str, Any],
        id_path: Path | None,
        cursor_path: FieldPath | NestedPath | None,
    ) -> None:
        self._record = template
        self._id_path = id_path
        self._cursor_path = cursor_path

        self._validate_template()

    def _validate_template(self) -> None:
        paths_to_validate = [
            ("_id_path", self._id_path),
            ("_cursor_path", self._cursor_path),
        ]
        for field_name, field_path in paths_to_validate:
            self._validate_field(field_name, field_path)

    def _validate_field(self, field_name: str, path: Path | None) -> None:
        try:
            if path and not path.extract(self._record):
                raise ValueError(
                    f"{field_name} `{path}` was provided but it is not part of the template `{self._record}`"
                )
        except (IndexError, KeyError) as exception:
            raise ValueError(
                f"{field_name} `{path}` was provided but it is not part of the template `{self._record}`"
            ) from exception

    def with_id(self, identifier: Any) -> RecordBuilder:  # noqa: ANN401  (any-type)
        self._set_field("id", self._id_path, identifier)
        return self

    def with_cursor(self, cursor_value: Any) -> RecordBuilder:  # noqa: ANN401  (any-type)
        self._set_field("cursor", self._cursor_path, cursor_value)
        return self

    def with_field(self, path: Path, value: Any) -> RecordBuilder:  # noqa: ANN401  (any-type)
        path.write(self._record, value)
        return self

    def _set_field(self, field_name: str, path: Path | None, value: Any) -> None:  # noqa: ANN401  (any-type)
        if not path:
            raise ValueError(
                f"{field_name}_path was not provided and hence, the record {field_name} can't be modified. Please provide `id_field` while "
                f"instantiating RecordBuilder to leverage this capability"
            )
        path.update(self._record, value)

    def build(self) -> dict[str, Any]:
        return self._record


class HttpResponseBuilder:
    def __init__(
        self,
        template: dict[str, Any],
        records_path: FieldPath | NestedPath,
        pagination_strategy: PaginationStrategy | None,
    ) -> None:
        self._response = template
        self._records: list[RecordBuilder] = []
        self._records_path = records_path
        self._pagination_strategy = pagination_strategy
        self._status_code = 200

    def with_record(self, record: RecordBuilder) -> HttpResponseBuilder:
        self._records.append(record)
        return self

    def with_pagination(self) -> HttpResponseBuilder:
        if not self._pagination_strategy:
            raise ValueError(
                "`pagination_strategy` was not provided and hence, fields related to the pagination can't be modified. Please provide "
                "`pagination_strategy` while instantiating ResponseBuilder to leverage this capability"
            )
        self._pagination_strategy.update(self._response)
        return self

    def with_status_code(self, status_code: int) -> HttpResponseBuilder:
        self._status_code = status_code
        return self

    def build(self) -> HttpResponse:
        self._records_path.update(self._response, [record.build() for record in self._records])
        return HttpResponse(json.dumps(self._response), self._status_code)


def _get_unit_test_folder(execution_folder: str) -> FilePath:
    # FIXME: This function should be removed after the next CDK release to avoid breaking amazon-seller-partner test code.  # noqa: FIX001, TD001
    return get_unit_test_folder(execution_folder)  # type: ignore # get_unit_test_folder is known to return a FilePath


def find_template(resource: str, execution_folder: str) -> dict[str, Any]:
    response_template_filepath = str(
        get_unit_test_folder(execution_folder)
        / "resource"
        / "http"
        / "response"
        / f"{resource}.json"
    )
    with open(response_template_filepath, encoding="utf-8") as template_file:  # noqa: PTH123  (prefer pathlib)
        return json.load(template_file)  # type: ignore  # we assume the dev correctly set up the resource file


def create_record_builder(
    response_template: dict[str, Any],
    records_path: FieldPath | NestedPath,
    record_id_path: Path | None = None,
    record_cursor_path: FieldPath | NestedPath | None = None,
) -> RecordBuilder:
    """This will use the first record define at `records_path` as a template for the records. If more records are defined, they will be ignored"""
    try:
        record_template = records_path.extract(response_template)[0]
        if not record_template:
            raise ValueError(
                f"Could not extract any record from template at path `{records_path}`. "
                f"Please fix the template to provide a record sample or fix `records_path`."
            )
        return RecordBuilder(record_template, record_id_path, record_cursor_path)
    except (IndexError, KeyError):
        raise ValueError(
            f"Error while extracting records at path `{records_path}` from response template `{response_template}`"
        ) from None


def create_response_builder(
    response_template: dict[str, Any],
    records_path: FieldPath | NestedPath,
    pagination_strategy: PaginationStrategy | None = None,
) -> HttpResponseBuilder:
    return HttpResponseBuilder(response_template, records_path, pagination_strategy)
