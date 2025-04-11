#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import hashlib
import json
from collections import defaultdict
from itertools import chain
from typing import Any, Callable, DefaultDict, Dict, Iterable, List, Optional, Tuple

from airbyte_cdk.sources.declarative.parsers.custom_exceptions import ManifestNormalizationException

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


def _get_sharable_schema_tags(schema: DefinitionsType) -> List[str]:
    """
    Extracts sharable tags from schema definitions.
    This function identifies properties within a schema's definitions that are marked as sharable.
    It traverses through each definition in the schema, examines its properties, and collects
    the keys of properties that contain the SHARABLE_TAG.

    Args:
        schema (DefinitionsType): The schema definition dictionary to process

    Returns:
        List[str]: A deduplicated list of property keys that are marked as sharable
    """

    # the sharable scope: ['definitions.*']
    schema_definitions = schema.get(DEF_TAG, {})

    sharable_tags: List[str] = []
    # Extract sharable keys from properties

    extract_sharable_keys: Callable[[Dict[str, Dict[str, Any]]], List[str]] = lambda properties: [
        key for key, value in properties.items() if SHARABLE_TAG in value.keys()
    ]

    # Process each root value to get its sharable keys
    process_root: Callable[[Dict[str, Any]], List[str]] = lambda root_value: extract_sharable_keys(
        root_value.get(PROPERTIES_TAG, {})
    )

    # Map the process_root function over all schema values and flatten the results
    all_sharable_tags = chain.from_iterable(map(process_root, schema_definitions.values()))

    # Add all found sharable tags to the tags list
    sharable_tags.extend(all_sharable_tags)

    # return unique tags only
    return list(set(sharable_tags))


class ManifestNormalizer:
    """
    This class is responsible for normalizing the manifest by appliying processing such as:
     - removing duplicated definitions
     - replacing them with references.

    To extend the functionality, use the `normilize()` method to include any additional processing steps.
    """

    def __init__(
        self,
        resolved_manifest: ManifestType,
        declarative_schema: DefinitionsType,
    ) -> None:
        self._resolved_manifest = resolved_manifest
        self._declarative_schema = declarative_schema
        self._normalized_manifest: ManifestType = copy.deepcopy(self._resolved_manifest)
        # get the tags marked as `sharable` in the component schema
        self._sharable_tags = _get_sharable_schema_tags(self._declarative_schema)

    def to_json_str(self) -> str:
        return json.dumps(self._normalized_manifest, indent=2)

    def normalize(self) -> ManifestType:
        """
        Normalizes the manifest by deduplicating and resolving schema references.

        This method processes the manifest in two steps:
        1. Deduplicates elements within the manifest
        2. Resolves and references schemas

        Returns:
            ManifestType: The normalized manifest if processing succeeds,
                          or the original resolved manifest if normalization fails.

        Raises:
            ManifestNormalizationException: Caught internally and handled by returning the original manifest.
        """
        try:
            self._deduplicate_minifest()
            self._reference_schemas()

            return self._normalized_manifest
        except ManifestNormalizationException:
            # if any error occurs, we just return the original manifest.
            return self._resolved_manifest

    def _get_manifest_streams(self) -> Iterable[Dict[str, Any]]:
        """
        Get the streams from the manifest.

        Returns:
            An Iterable of streams.
        """

        if STREAMS_TAG in self._normalized_manifest.keys():
            for stream in self._normalized_manifest[STREAMS_TAG]:
                yield stream

        yield from []

    def _deduplicate_minifest(self) -> None:
        """
        Find commonalities in the input JSON structure and refactor it to avoid redundancy.
        """

        try:
            # prepare the `definitions` tag
            self._prepare_definitions()
            # replace duplicates with references, if any
            self._handle_duplicates(self._collect_duplicates())
        except Exception as e:
            raise ManifestNormalizationException(str(e))

    def _prepare_definitions(self) -> None:
        """
        Clean the definitions in the manifest by removing unnecessary properties.
        This function modifies the manifest in place.
        """

        # Check if the definitions tag exists
        if not DEF_TAG in self._normalized_manifest:
            self._normalized_manifest[DEF_TAG] = {}

        # Check if the shared tag exists
        if not SHARED_TAG in self._normalized_manifest[DEF_TAG]:
            self._normalized_manifest[DEF_TAG][SHARED_TAG] = {}

        # remove everything from definitions tag except of `shared`, after processing
        for key in list(self._normalized_manifest[DEF_TAG].keys()):
            if key != SHARED_TAG:
                self._normalized_manifest[DEF_TAG].pop(key, None)

    def _reference_schemas(self) -> None:
        """
        Process the definitions in the manifest to move streams from definitions to the main stream list.
        This function modifies the manifest in place.
        """

        # reference the stream schema for the stream to where it's stored
        if SCHEMAS_TAG in self._normalized_manifest.keys():
            for stream in self._get_manifest_streams():
                stream_name = stream["name"]

                if stream_name not in self._normalized_manifest[SCHEMAS_TAG].keys():
                    raise ManifestNormalizationException(
                        f"Stream {stream_name} not found in `schemas`. Please check the manifest."
                    )

                stream[SCHEMA_LOADER_TAG][SCHEMA_TAG] = self._create_schema_ref(stream_name)

    def _replace_duplicates_with_refs(self, duplicates: DuplicatesType) -> None:
        """
        Process duplicate objects and replace them with references.

        Args:
            duplicates: The duplicates dictionary collected from the given manifest.
        """

        for _, occurrences in duplicates.items():
            type_key, key, value = self._get_occurance_samples(occurrences)
            is_shared_def = self._is_shared_definition(type_key, key)

            # Add to definitions if not there already
            if not is_shared_def:
                self._add_to_shared_definitions(type_key, key, value)

            # Replace occurrences with references
            for _, parent_obj, value in occurrences:
                if is_shared_def:
                    if value == self._get_shared_definition_value(type_key, key):
                        parent_obj[key] = self._create_shared_definition_ref(type_key, key)
                else:
                    parent_obj[key] = self._create_shared_definition_ref(type_key, key)

    def _handle_duplicates(self, duplicates: DuplicatesType) -> None:
        """
        Process the duplicates and replace them with references.

        Args:
            duplicates: The duplicates dictionary collected from the given manifest.
        """

        if len(duplicates) > 0:
            self._replace_duplicates_with_refs(duplicates)

    def _add_duplicate(
        self,
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

        # create hash for each duplicate observed
        value_to_hash = {key: value} if key is not None else value
        duplicates[self._hash_object(value_to_hash)].append((current_path, obj, value))

    def _add_to_shared_definitions(
        self,
        type_key: str,
        key: str,
        value: Any,
    ) -> None:
        """
        Add a value to the shared definitions under the specified key.

        Args:
            definitions: The definitions dictionary to modify
            key: The key to use
            value: The value to add
        """
        if type_key not in self._normalized_manifest[DEF_TAG][SHARED_TAG].keys():
            self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key] = {}

        if key not in self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key].keys():
            self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key][key] = value

    def _collect_duplicates(self) -> DuplicatesType:
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
                    if key in self._sharable_tags:
                        self._add_duplicate(duplicates, current_path, obj, value)

                # handle primitive types
                elif isinstance(value, (str, int, float, bool)):
                    # Process allowed-only field tags
                    if key in self._sharable_tags:
                        self._add_duplicate(duplicates, current_path, obj, value, key)

                # handle list cases
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        _collect(item, current_path + [str(i)])

        duplicates: DuplicatesType = defaultdict(list, {})
        try:
            if self._sharable_tags:
                _collect(self._normalized_manifest)
                # clean non-duplicates and sort based on the count of occurrences
                return self._clean_and_sort_duplicates(duplicates)
            return duplicates
        except Exception as e:
            raise ManifestNormalizationException(str(e))

    def _clean_and_sort_duplicates(self, duplicates: DuplicatesType) -> DuplicatesType:
        """
        Clean non-duplicates and sort the duplicates by their occurrences.

        Args:
            duplicates: The duplicates dictionary to sort

        Returns:
            A sorted duplicates dictionary.
        """

        # clean non-duplicates
        duplicates = defaultdict(
            list,
            {k: v for k, v in duplicates.items() if len(v) >= N_OCCURANCES},
        )

        # sort the duplicates by their occurrences, more frequent ones go first
        duplicates = defaultdict(
            list,
            {k: v for k, v in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)},
        )

        return duplicates

    def _hash_object(self, obj: Dict[str, Any]) -> str:
        """
        Create a unique hash for a dictionary object.

        Args:
            node: The dictionary to hash

        Returns:
            A hashed string
        """

        # Sort keys to ensure consistent hash for same content
        return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).hexdigest()

    def _is_shared_definition(self, type_key: str, key: str) -> bool:
        """
        Check if the key already exists in the shared definitions.

        Args:
            key: The key to check
            definitions: The definitions dictionary with definitions

        Returns:
            True if the key exists in the shared definitions, False otherwise
        """

        if type_key in self._normalized_manifest[DEF_TAG][SHARED_TAG].keys():
            # Check if the key exists in the shared definitions
            if key in self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key].keys():
                return True

        return False

    def _get_shared_definition_value(self, type_key: str, key: str) -> Any:
        """
        Get the value of a shared definition by its key.

        Args:
            key: The key to check
            definitions: The definitions dictionary with definitions

        Returns:
            The value of the shared definition
        """
        if type_key in self._normalized_manifest[DEF_TAG][SHARED_TAG].keys():
            if key in self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key].keys():
                return self._normalized_manifest[DEF_TAG][SHARED_TAG][type_key][key]
        else:
            raise ManifestNormalizationException(
                f"Key {key} not found in shared definitions. Please check the manifest."
            )

    def _get_occurance_samples(self, occurrences: DuplicateOccurancesType) -> Tuple[str, str, Any]:
        """
        Get the key from the occurrences list.

        Args:
            occurrences: The occurrences list

        Returns:
            The key, type and value from the occurrences
        """

        # Take the value from the first occurrence, as they are the same
        path, obj, value = occurrences[0]
        return (
            obj["type"],
            path[-1],
            value,
        )  # Return the component's name as the last part of its path

    def _create_shared_definition_ref(self, type_key: str, key: str) -> Dict[str, str]:
        """
        Create a reference object for the shared definitions using the specified key.

        Args:
            ref_key: The reference key to use

        Returns:
            A reference object in the proper format
        """

        return {"$ref": f"#/{DEF_TAG}/{SHARED_TAG}/{type_key}/{key}"}

    def _create_schema_ref(self, ref_key: str) -> Dict[str, str]:
        """
        Create a reference object for stream schema using the specified key.

        Args:
            ref_key: The reference key to use

        Returns:
            A reference object in the proper format
        """

        return {"$ref": f"#/{SCHEMAS_TAG}/{ref_key}"}
