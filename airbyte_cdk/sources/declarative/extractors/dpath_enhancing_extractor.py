#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Union

from airbyte_cdk.sources.declarative.extractors.dpath_extractor import DpathExtractor
from airbyte_cdk.sources.declarative.extractors.record_extractor import (
    SERVICE_KEY_PREFIX,
    add_service_key,
    is_service_key,
)

# The name of the service key that holds a reference to the original root response
SERVICE_KEY_ROOT = "root"

# The name of the service key that holds a reference to the owner object
SERVICE_KEY_PARENT = "parent"


@dataclass
class DpathEnhancingExtractor(DpathExtractor):
    """
    Navigate a path through the JSON structure to the records to retrieve. Extend the records with service fields
    applicable to their filtering and transformation.

    Like the DpathExtractor, extract records from a response by following a path of names  of nested objects,
    while adding specific service fields to the extracted records to facilitate the further processing.

    Service fields:
      root: Binds the original response body, the record was extracted from. This allows the record access any attribute
      in any nested object, navigating from the root field.

      parent: Binds a map of the parent object's attributes, including its "parent" service field. This way the extracted
      record has access to the attributes of any object This is especially useful when the records are extracted from
      nested lists.

    Example:
      body: {"a":1, "b":2, "c":{"d":4}}\n
      path: {c}\n
      record: {"d":4,"parent": { "a":1, "b":2}, "root": { "a":1, "b":2, "c":{"d":4}}}\n
      access: {{ record.d }}, {{ record["parent"].a }}, {{ record["parent"].b }}, {{ record.["root"].a }}...

    Example:
      body: {"a":1, "b":2, "c":[{"d":4},{"e",5}]}\n
      path: {c, *}\n
      record 1: {"d":4, "parent":{ "a":1, "b":2}, "root":{ "a":1, "b":2, "c":[{"d":4},{"e",5}]})\n
      record 2: {"e",5, "parent":{ "a":1, "b":2}, "root":{ "a":1, "b":2, "c":[{"d":4},{"e",5}]})\n
      access: {{ record.d }}, {{ record["parent"].a }}, {{ record["parent"].b }}, {{ record.["root"].a }}...

    Example:
      body: { "a":1, "b":2, "c":{"d":4, "e":{"f":6}}}\n
      path: {c,e}\n
      record: {"f":6, "parent": {"d":4, parent: { "a":1, "b":2}},"root":{ "a":1, "b":2, "c":{"d":4, "e":{"f":6}}}}\n
      access: {{ record.f }}, {{ record["parent"].d }}, {{ record["parent"]["parent"].a }},\n
      {{ record["parent"]["parent"].b }},{{ record.["root"].a }}, {{ record.["root"].a.c.d }}...

    Note:
        The names of the service fields have a specific prefix like $ set in SERVICE_KEY_PREFIX.\n
        When the result record is the body object itself, then the "parent" service field is not set (as it is None).\n
        When the parent contains no attributes and no parent service field, the parent field is not bound.\n
        The "root" service field is always set in the result record.
    """

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        """
        See  DpathExtractor
        """
        super().__post_init__(parameters)

    def update_body(self, body: Any) -> Any:
        """
        In each nested object in the body add a service key "parent" to refer to the owner object.
        For the root object/body the owner is None.
        Example:
        body = { "a":1, "b":2, "c":{"d":4}}
        result = { "a":1,
                   "b":2,
                   "c":{"d":4,
                        parent: { "a":1, "b":2}}}

        Example:
        body = { "a":1, "b":2, "c":[{"d":4},{"e",5}]}
        result = { "a":1,
                   "b":2,
                   "c":[{"d":4, "parent":{ "a":1, "b":2}},
                        {"e",5, "parent":{ "a":1, "b":2}}],
                 }

        Example:
        body = { "a":1, "b":2, "c":{"d":4, "e":{"f":6}}}
        result = { "a":1,
                   "b":2,
                   "c":{"d":4,
                        parent: { "a":1, "b":2},
                        "e":{"f":6,
                             "parent": {"d":4,
                                        parent: { "a":1, "b":2}} }}}

        :param body: the original response body. Not to be changed
        :return: a copy of the body, where the nested objects have the "parent" service field bound to the map of the
                 parent object's attributes (including its "parent" service fields). This way any record that will be
                 extracted from the nested objects will have access to any parent's attributes still avoiding loops
                 in the JSON structure.
        """
        return self._set_parent(body, None)

    def _set_parent(self, body: Any, parent: Any) -> Any:
        """
        :param body: the original response body. Not to be changed
        :param parent: none or the parent object that owns/has as nested the body object
        :return: a copy of the body enhanced in a subclass-specific way. None only when body is None.
        """
        if isinstance(body, dict):
            result: dict[str, Any] = dict()
            if parent:
                result = add_service_key(result, SERVICE_KEY_PARENT, parent)
            attributes_only = dict(result)
            attributes_only.update(
                {
                    k: v
                    for k, v in body.items()
                    if v and not isinstance(v, dict) and not isinstance(v, list)
                }
            )
            for k, v in body.items():
                result[k] = self._set_parent(v, attributes_only)
            return result
        elif isinstance(body, list):
            return [self._set_parent(v, parent) for v in body]
        else:
            return body

    def update_record(self, record: Any, root: Any) -> Any:
        """
        Change the extracted record in a subclass-specific way. Override in subclasses.
        :param record: the original extracted record. Not to be changed. Not None.
        :param root: the original body the record is extracted from.
        :return: a copy of the record changed or enanced in a subclass-specific way.
        """
        return add_service_key(record, SERVICE_KEY_ROOT, root)
