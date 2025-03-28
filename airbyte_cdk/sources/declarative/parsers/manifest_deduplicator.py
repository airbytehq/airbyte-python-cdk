#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import hashlib
import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Tuple

from airbyte_cdk.sources.declarative.parsers.custom_exceptions import ManifestDeduplicationException

# Type definitions for better readability
ManifestType = Dict[str, Any]
DefinitionsType = Dict[str, Any]
DuplicatesType = DefaultDict[str, List[Tuple[List[str], Dict[str, Any], Dict[str, Any]]]]

# Configuration constants
N_OCCURANCES = 2

DEF_TAG = "definitions"
SHARED_TAG = "shared"

# SPECIFY TAGS FOR DEDUPLICATION
TAGS = [
    "authenticator",
    "url_base",
]


def deduplicate_definitions(resolved_manifest: ManifestType) -> ManifestType:
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
        definitions = _manifest.get(DEF_TAG, {})

        duplicates = _collect_duplicates(definitions)
        _handle_duplicates(definitions, duplicates)

        return _manifest
    except ManifestDeduplicationException:
        # if any arror occurs, we just return the original manifest.
        return resolved_manifest


def _replace_duplicates_with_refs(definitions: ManifestType, duplicates: DuplicatesType) -> None:
    """
    Process duplicate objects and replace them with references.

    Args:
        definitions: The definitions dictionary to modify
    """
    for _, occurrences in duplicates.items():
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


def _handle_duplicates(definitions: DefinitionsType, duplicates: DuplicatesType) -> None:
    """
    Process the duplicates and replace them with references.

    Args:
        duplicates: Dictionary of duplicate objects
    """
    # process duplicates only if there are any
    if len(duplicates) > 0:
        if not SHARED_TAG in definitions:
            definitions[SHARED_TAG] = {}

        try:
            _replace_duplicates_with_refs(definitions, duplicates)
        except Exception as e:
            raise ManifestDeduplicationException(str(e))


def _is_allowed_tag(key: str) -> bool:
    """
    Check if the key is an allowed tag for deduplication.

    Args:
        key: The key to check

    Returns:
        True if the key is allowed, False otherwise
    """
    return key in TAGS


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
            current_path = path + [key]

            if isinstance(value, dict):
                # First process nested dictionaries
                _collect(value, current_path)
                # Process allowed-only component tags
                if _is_allowed_tag(key):
                    _add_duplicate(duplicates, current_path, obj, value)

            # handle primitive types
            elif isinstance(value, (str, int, float, bool)):
                # Process allowed-only field tags
                if _is_allowed_tag(key):
                    _add_duplicate(duplicates, current_path, obj, value, key)

            # handle list cases
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    _collect(item, current_path + [str(i)])

    duplicates: DuplicatesType = defaultdict(list, {})

    try:
        _collect(node)
        return duplicates
    except Exception as e:
        raise ManifestDeduplicationException(str(e))


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


def _create_reference_key(definitions: DefinitionsType, key: str) -> str:
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
