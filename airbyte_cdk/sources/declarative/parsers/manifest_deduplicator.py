#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import hashlib
import json
import pkgutil
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Tuple

import yaml

from airbyte_cdk.sources.declarative.parsers.custom_exceptions import ManifestDeduplicationException

# Type definitions for better readability
ManifestType = Dict[str, Any]
DefinitionsType = Dict[str, Any]
DuplicateOccurancesType = List[Tuple[List[str], Dict[str, Any], Dict[str, Any]]]
DuplicatesType = DefaultDict[str, DuplicateOccurancesType]

# Configuration constants
N_OCCURANCES = 2

DEF_TAG = "definitions"
STREAMS_TAG = "streams"
SHARED_TAG = "shared"
SHARABLE_TAG = "sharable"
SCHEMA_LOADER_TAG = "schema_loader"
SCHEMAS_TAG = "schemas"
SCHEMA_TAG = "schema"
PROPERTIES_TAG = "properties"


def deduplicate_minifest(resolved_manifest: ManifestType) -> ManifestType:
    """
    Find commonalities in the input JSON structure and refactor it to avoid redundancy.

    Args:
        resolved_manifest: A dictionary representing a JSON structure to be analyzed.

    Returns:
        A refactored JSON structure with common properties extracted to `definitions.shared`,
        the duplicated properties replaced with references
    """

    try:
        _manifest = copy.deepcopy(resolved_manifest)

        # prepare the `definitions` tag
        _prepare_definitions(_manifest)
        # replace duplicates with references, if any
        _handle_duplicates(_manifest, _collect_duplicates(_manifest))
        # post processing the manifest
        _reference_schemas(_manifest)

        return _manifest
    except ManifestDeduplicationException:
        # if any error occurs, we just return the original manifest.
        return resolved_manifest


def _get_sharable_tags() -> List[str]:
    # we need to recursively find the tags in the component schema that has a `sharable` key inside.
    tags = []

    # we need to find the `sharable` tags in the component schema
    def _find_sharable(schema: Dict[str, Any]) -> None:
        for root_key, root_value in schema.items():
            properties = root_value.get(PROPERTIES_TAG, {})

            for inner_key, inner_value in properties.items():
                if SHARABLE_TAG in inner_value.keys():
                    tags.append(inner_key)

    _find_sharable(_get_declarative_component_schema().get(DEF_TAG, {}))

    # return unique tags only
    return list(set(tags))


def _get_declarative_component_schema() -> Dict[str, Any]:
    try:
        raw_component_schema = pkgutil.get_data(
            "airbyte_cdk", "sources/declarative/declarative_component_schema.yaml"
        )
        if raw_component_schema is not None:
            declarative_component_schema = yaml.load(raw_component_schema, Loader=yaml.SafeLoader)
            return declarative_component_schema  # type: ignore
        else:
            raise RuntimeError(
                "Failed to read manifest component json schema required for deduplication"
            )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Failed to read manifest component json schema required for deduplication: {e}"
        )


def _prepare_definitions(manifest: ManifestType) -> None:
    """
    Clean the definitions in the manifest by removing unnecessary properties.
    This function modifies the manifest in place.
    Args:
        manifest: The manifest to clean
    """
    # Check if the definitions tag exists
    if not DEF_TAG in manifest:
        manifest[DEF_TAG] = {}

    # remove everything from definitions tag except of `shared`, after processing
    for key in list(manifest[DEF_TAG].keys()):
        if key != SHARED_TAG:
            manifest[DEF_TAG].pop(key, None)


def _reference_schemas(manifest: ManifestType) -> None:
    """
    Process the definitions in the manifest to move streams from definitions to the main stream list.
    This function modifies the manifest in place.

    Args:
        manifest: The manifest to process
    """

    # create the ref tag to each stream in the manifest
    if STREAMS_TAG in manifest:
        for stream in manifest[STREAMS_TAG]:
            stream_name = stream.get("name")
            # reference the stream schema for the stream to where it's storred
            if stream_name in manifest[SCHEMAS_TAG].keys():
                stream[SCHEMA_LOADER_TAG][SCHEMA_TAG] = _create_schema_ref(stream_name)
            else:
                raise ManifestDeduplicationException(
                    f"Stream {stream_name} not found in `schemas`. Please check the manifest."
                )


def _replace_duplicates_with_refs(manifest: ManifestType, duplicates: DuplicatesType) -> None:
    """
    Process duplicate objects and replace them with references.

    Args:
        definitions: The definitions dictionary to modify
    """
    for _, occurrences in duplicates.items():
        # take the component's name as the last part of it's path
        type_key, key, value = _get_key_value_from_occurances(occurrences)
        is_shared_def = _is_shared_definition(manifest, type_key, key)

        # Add to definitions if not there already
        if not is_shared_def:
            _add_to_shared_definitions(manifest, type_key, key, value)

        # Replace occurrences with references
        for path, parent_obj, value in occurrences:
            if is_shared_def:
                if value == _get_shared_definition_value(manifest, type_key, key):
                    parent_obj[key] = _create_shared_definition_ref(type_key, key)
            else:
                parent_obj[key] = _create_shared_definition_ref(type_key, key)


def _handle_duplicates(manifest: DefinitionsType, duplicates: DuplicatesType) -> None:
    """
    Process the duplicates and replace them with references.

    Args:
        duplicates: Dictionary of duplicate objects
    """

    if len(duplicates) > 0:
        # Check if the shared tag exists
        if not SHARED_TAG in manifest[DEF_TAG]:
            manifest[DEF_TAG][SHARED_TAG] = {}

        try:
            _replace_duplicates_with_refs(manifest, duplicates)
        except Exception as e:
            raise ManifestDeduplicationException(str(e))


def _add_duplicate(
    duplicates: DuplicatesType,
    current_path: List[str],
    obj: Dict[str, Any],
    value: Any,
    key: Optional[str] = None,
) -> None:
    """
    Adds a duplicate record of an observed object by computing a unique hash for the provided value.

    This function computes a hash for the given value (or a dictionary composed of the key and value if a key is provided)
    and appends a tuple containing the current path, the original object, and the value to the duplicates
    dictionary under the corresponding hash.

    Parameters:
        duplicates (DuplicatesType): The dictionary to store duplicate records.
        current_path (List[str]): The list of keys or indices representing the current location in the object hierarchy.
        obj (Dict): The original dictionary object where the duplicate is observed.
        value (Any): The value to be hashed and used for identifying duplicates.
        key (Optional[str]): An optional key that, if provided, wraps the value in a dictionary before hashing.
    """
    # create hash for the duplicate observed
    value_to_hash = value if key is None else {key: value}
    obj_hash = _hash_object(value_to_hash)
    if obj_hash:
        duplicates[obj_hash].append((current_path, obj, value))


def _add_to_shared_definitions(
    manifest: DefinitionsType,
    type_key: str,
    key: str,
    value: Any,
) -> DefinitionsType:
    """
    Add a value to the shared definitions under the specified key.

    Args:
        definitions: The definitions dictionary to modify
        key: The key to use
        value: The value to add
    """
    if type_key not in manifest[DEF_TAG][SHARED_TAG].keys():
        manifest[DEF_TAG][SHARED_TAG][type_key] = {}

    if key not in manifest[DEF_TAG][SHARED_TAG][type_key].keys():
        manifest[DEF_TAG][SHARED_TAG][type_key][key] = value

    return manifest


def _collect_duplicates(node: ManifestType) -> DuplicatesType:
    """
    Traverse the JSON object and collect all potential duplicate values and objects.

    Args:
        node: The JSON object to analyze.

    Returns:
        duplicates: A dictionary of duplicate objects.
    """

    def _collect(obj: Dict[str, Any], path: Optional[List[str]] = None) -> None:
        """
        The closure to recursively collect duplicates in the JSON object.

        Args:
            obj: The current object being analyzed.
            path: The current path in the object hierarchy.
        """

        if not isinstance(obj, dict):
            return

        path = [] if path is None else path
        # Check if the object is empty
        for key, value in obj.items():
            # do not collect duplicates from `definitions` tag
            if key == DEF_TAG:
                continue

            current_path = path + [key]

            if isinstance(value, dict):
                # First process nested dictionaries
                _collect(value, current_path)
                # Process allowed-only component tags
                if key in sharable_tags:
                    _add_duplicate(duplicates, current_path, obj, value)

            # handle primitive types
            elif isinstance(value, (str, int, float, bool)):
                # Process allowed-only field tags
                if key in sharable_tags:
                    _add_duplicate(duplicates, current_path, obj, value, key)

            # handle list cases
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    _collect(item, current_path + [str(i)])

    # get the tags marked as `sharable` in the component schema
    sharable_tags = _get_sharable_tags()

    duplicates: DuplicatesType = defaultdict(list, {})
    try:
        if sharable_tags:
            _collect(node)
            # clean non-duplicates and sort based on the count of occurrences
            return clean_and_sort_duplicates(duplicates)
        return duplicates
    except Exception as e:
        raise ManifestDeduplicationException(str(e))


def clean_and_sort_duplicates(duplicates: DuplicatesType) -> DuplicatesType:
    """
    Clean non-duplicates and sort the duplicates by their occurrences.

    Args:
        duplicates: The duplicates dictionary to sort
    Returns:
        A sorted duplicates dictionary
    """
    # clean non-duplicates
    duplicates = defaultdict(list, {k: v for k, v in duplicates.items() if len(v) >= N_OCCURANCES})
    # sort the duplicates by their occurrences
    return defaultdict(
        list, {k: v for k, v in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)}
    )


def _hash_object(node: Dict[str, Any]) -> Optional[str]:
    """
    Create a unique hash for a dictionary object.

    Args:
        node: The dictionary to hash

    Returns:
        A hash string or None if not hashable
    """
    if isinstance(node, Dict):
        # Sort keys to ensure consistent hash for same content
        return hashlib.md5(json.dumps(node, sort_keys=True).encode()).hexdigest()
    return None


def _is_shared_definition(manifest: DefinitionsType, type_key: str, key: str) -> bool:
    """
    Check if the key already exists in the shared definitions.

    Args:
        key: The key to check
        definitions: The definitions dictionary with definitions

    Returns:
        True if the key exists in the shared definitions, False otherwise
    """

    if type_key in manifest[DEF_TAG][SHARED_TAG].keys():
        # Check if the key exists in the shared definitions
        if key in manifest[DEF_TAG][SHARED_TAG][type_key].keys():
            return True

    return False


def _get_shared_definition_value(manifest: DefinitionsType, type_key: str, key: str) -> Any:
    """
    Get the value of a shared definition by its key.

    Args:
        key: The key to check
        definitions: The definitions dictionary with definitions
    Returns:
        The value of the shared definition
    """
    if type_key in manifest[DEF_TAG][SHARED_TAG].keys():
        if key in manifest[DEF_TAG][SHARED_TAG][type_key].keys():
            return manifest[DEF_TAG][SHARED_TAG][type_key][key]
    else:
        raise ManifestDeduplicationException(
            f"Key {key} not found in shared definitions. Please check the manifest."
        )


def _get_key_value_from_occurances(occurrences: DuplicateOccurancesType) -> Tuple[str, str, Any]:
    """
    Get the key from the occurrences list.

    Args:
        occurrences: The occurrences list

    Returns:
        The key, type and value from the occurrences
    """

    # Take the value from the first occurrence, as they are the same
    path, obj, value = occurrences[0]
    return obj["type"], path[-1], value  # Return the component's name as the last part of its path


def _create_shared_definition_ref(type_key: str, key: str) -> Dict[str, str]:
    """
    Create a reference object for the shared definitions using the specified key.

    Args:
        ref_key: The reference key to use

    Returns:
        A reference object in the proper format
    """
    return {"$ref": f"#/{DEF_TAG}/{SHARED_TAG}/{type_key}/{key}"}


def _create_schema_ref(ref_key: str) -> Dict[str, str]:
    """
    Create a reference object for stream schema using the specified key.

    Args:
        ref_key: The reference key to use

    Returns:
        A reference object in the proper format
    """
    return {"$ref": f"#/{SCHEMAS_TAG}/{ref_key}"}
