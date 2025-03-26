#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import hashlib
import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Hashable, List, Optional, Tuple

from airbyte_cdk.sources.declarative.parsers.custom_exceptions import ManifestDeduplicationException

# Type definitions for better readability
ManifestType = Dict[str, Any]
DefinitionsType = Dict[str, Any]
FieldDuplicatesType = DefaultDict[Tuple[str, Any], List[Tuple[List[str], Dict]]]
ComponentDuplicatesType = DefaultDict[str, List[Tuple[List[str], Dict, Dict]]]

# Configuration constants
N_OCCURANCES = 2

DEF_TAG = "definitions"
SHARED_TAG = "shared"

# SPECIFY COMPONENT TAGS FOR DEDUPLICATION
COMPONENT_TAGS = [
    "authenticator",
]

# SPECIFY FIELD TAGS FOR DEDUPLICATION
FIELD_TAGS = [
    "url_base",
]


def deduplicate_definitions(resolved_manifest: ManifestType) -> ManifestType:
    """
    Find commonalities in the input JSON structure and refactor it to avoid redundancy.

    Args:
        resolved_manifest: A dictionary representing a JSON structure to be analyzed.

    Returns:
        A refactored JSON structure with common properties extracted.
    """

    try:
        _manifest = copy.deepcopy(resolved_manifest)
        definitions = _manifest.get(DEF_TAG, {})
        field_duplicates, component_duplicates = _collect_all_duplicates(definitions)
        _process_duplicates(definitions, field_duplicates, component_duplicates)
        return _manifest
    except ManifestDeduplicationException:
        # we don't want to fix every single error which might occur,
        # due to the varaety of possible manifest configurations,
        # if any arror occurs, we just return the original manifest.
        return resolved_manifest


def _process_duplicates(
    definitions: DefinitionsType,
    field_duplicates: FieldDuplicatesType,
    component_duplicates: ComponentDuplicatesType,
) -> None:
    """
    Process the duplicates and replace them with references.

    Args:
        field_duplicates: Dictionary of duplicate primitive values
        component_duplicates: Dictionary of duplicate objects
    """
    # process duplicates only if there are any
    if len(field_duplicates) > 0 or len(component_duplicates) > 0:
        if not SHARED_TAG in definitions:
            definitions[SHARED_TAG] = {}

        try:
            _process_component_duplicates(definitions, component_duplicates)
            _process_field_duplicates(definitions, field_duplicates)
        except Exception as e:
            raise ManifestDeduplicationException(str(e))


def _is_allowed_component(key: str) -> bool:
    """
    Check if the key is an allowed component tag.

    Args:
        key: The key to check

    Returns:
        True if the key is allowed, False otherwise
    """
    return key in COMPONENT_TAGS


def _is_allowed_field(key: str) -> bool:
    """
    Check if the key is an allowed field tag.

    Args:
        key: The key to check

    Returns:
        True if the key is allowed, False otherwise
    """
    return key in FIELD_TAGS


def _collect_all_duplicates(
    node: ManifestType,
) -> Tuple[FieldDuplicatesType, ComponentDuplicatesType]:
    """
    Traverse the JSON object and collect all potential duplicate values and objects.

    Args:
        node: The JSON object to analyze

    Returns:
        A tuple of (field_duplicates, component_duplicates)
    """

    field_duplicates: FieldDuplicatesType = defaultdict(list)
    component_duplicates: ComponentDuplicatesType = defaultdict(list)

    def collect_duplicates(obj: Dict, path: Optional[List[str]] = None) -> None:
        if not isinstance(obj, dict):
            return

        path = [] if path is None else path
        # Check if the object is empty
        for key, value in obj.items():
            current_path = path + [key]

            if isinstance(value, dict):
                # First process nested dictionaries
                collect_duplicates(value, current_path)

                # Process allowed-only component tags
                if _is_allowed_component(key):
                    obj_hash = _hash_object(value)
                    if obj_hash:
                        component_duplicates[obj_hash].append((current_path, obj, value))

            # handle list[dict] cases
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    collect_duplicates(item, current_path + [str(i)])

            # Process allowed-only field tags
            elif _is_allowed_field(key):
                hashable_value = _make_hashable(value)
                field_duplicates[(key, hashable_value)].append((current_path, obj))

    try:
        collect_duplicates(node)
    except Exception as e:
        raise ManifestDeduplicationException(str(e))

    return field_duplicates, component_duplicates


def _hash_object(node: Dict) -> Optional[str]:
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


def _make_hashable(value: Any) -> Any:
    """
    Convert a value to a hashable representation.

    Args:
        value: The value to make hashable

    Returns:
        A hashable representation of the value
    """
    return json.dumps(value) if not isinstance(value, Hashable) else value


def _create_reference_key(
    definitions: DefinitionsType, key: str, value: Optional[Any] = None
) -> str:
    """
    Create a unique reference key and handle collisions.

    Args:
        key: The base key to use
        definitions: The definitions dictionary with definitions

    Returns:
        A unique reference key
    """

    counter = 1
    while key in definitions[SHARED_TAG]:
        # If the value is already in shared definitions with this key, no need to rename
        if value is not None and _is_same_value(definitions[SHARED_TAG].get(key), value):
            return key
        key = f"{key}_{counter}"
        counter += 1
    return key


def _create_ref_object(ref_key: str) -> Dict[str, str]:
    """
    Create a reference object using the specified key.

    Args:
        ref_key: The reference key to use

    Returns:
        A reference object in the proper format
    """
    return {"$ref": f"#/{DEF_TAG}/{SHARED_TAG}/{ref_key}"}


def _is_same_value(val1: Any, val2: Any) -> bool:
    """
    Check if two values are the same by comparing their JSON representation.

    Args:
        val1: First value
        val2: Second value

    Returns:
        True if the values are the same, False otherwise
    """
    return json.dumps(val1, sort_keys=True) == json.dumps(val2, sort_keys=True)


def _process_component_duplicates(
    definitions: ManifestType,
    component_duplicates: ComponentDuplicatesType,
) -> None:
    """
    Process duplicate objects and replace them with references.

    Args:
        definitions: The definitions dictionary to modify
        component_duplicates: Dictionary of duplicate objects
    """
    for obj_hash, occurrences in component_duplicates.items():
        # Skip non-duplicates
        if len(occurrences) < N_OCCURANCES:
            continue

        # Take the value from the first occurrence, as they are the same
        path, _, value = occurrences[0]
        # take the component's name as the last part of it's path
        key = path[-1]
        # Create a meaningful reference key
        ref_key = _create_reference_key(definitions, key)
        # Add to definitions
        _add_to_shared_definitions(definitions, ref_key, value)

        # Replace all occurrences with references
        for path, parent_obj, _ in occurrences:
            if path:  # Make sure the path is valid
                key = path[-1]
                parent_obj[key] = _create_ref_object(ref_key)


def _add_to_shared_definitions(
    definitions: DefinitionsType,
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

    if key not in definitions[SHARED_TAG]:
        definitions[SHARED_TAG][key] = value

    return definitions


def _process_field_duplicates(
    definitions: ManifestType,
    field_duplicates: FieldDuplicatesType,
) -> None:
    """
    Process duplicate primitive values and replace them with references.

    Args:
        definitions: The definitions dictionary to modify
        field_duplicates: Dictionary of duplicate primitive values
    """

    for (key, value), occurrences in field_duplicates.items():
        # Skip non-duplicates
        if len(occurrences) < N_OCCURANCES:
            continue

        ref_key = _create_reference_key(definitions, key, value)
        # Add to definitions if not already there
        _add_to_shared_definitions(definitions, ref_key, value)

        # Replace all occurrences with references
        for path, parent_obj in occurrences:
            if path:  # Make sure the path is valid
                key = path[-1]
                parent_obj[key] = _create_ref_object(ref_key)
