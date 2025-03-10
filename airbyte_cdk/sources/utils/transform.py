#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import importlib
import logging
import sys
from enum import Flag, auto
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Generator, Mapping, Optional, Type, cast

from datamodel_code_generator import DataModelType, InputFileType, generate
from jsonschema import Draft7Validator, RefResolver, ValidationError, Validator, validators
from pydantic import BaseModel

MAX_NESTING_DEPTH = 3
json_to_python_simple = {
    "string": str,
    "number": float,
    "integer": int,
    "boolean": bool,
    "null": type(None),
}
json_to_python = {**json_to_python_simple, **{"object": dict, "array": list}}
python_to_json = {v: k for k, v in json_to_python.items()}

logger = logging.getLogger("airbyte")

_TRUTHY_STRINGS = ("y", "yes", "t", "true", "on", "1")
_FALSEY_STRINGS = ("n", "no", "f", "false", "off", "0")


def _strtobool(value: str, /) -> int:
    """Mimic the behavior of distutils.util.strtobool.

    From: https://docs.python.org/2/distutils/apiref.html#distutils.util.strtobool

    > Convert a string representation of truth to true (1) or false (0).
    > True values are y, yes, t, true, on and 1; false values are n, no, f, false, off and 0. Raises
    > `ValueError` if val is anything else.
    """
    normalized_str = value.lower().strip()
    if normalized_str in _TRUTHY_STRINGS:
        return 1

    if normalized_str in _FALSEY_STRINGS:
        return 0

    raise ValueError(f"Invalid boolean value: {normalized_str}")


class TransformConfig(Flag):
    """
    TypeTransformer class config. Configs can be combined using bitwise or operator e.g.
        ```
        TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization
        ```
    """

    # No action taken, default behavior. Cannot be combined with any other options.
    NoTransform = auto()
    # Applies default type casting with default_convert method which converts
    # values by applying simple type casting to specified jsonschema type.
    DefaultSchemaNormalization = auto()
    # Allow registering custom type transformation callback. Can be combined
    # with DefaultSchemaNormalization. In this case default type casting would
    # be applied before custom one.
    CustomSchemaNormalization = auto()


class TypeTransformer:
    """
    Class for transforming object before output.
    """

    _custom_normalizer: Optional[Callable[[Any, Dict[str, Any]], Any]] = None

    def __init__(self, config: TransformConfig):
        """
        Initialize TypeTransformer instance.
        :param config Transform config that would be applied to object
        """
        if TransformConfig.NoTransform in config and config != TransformConfig.NoTransform:
            raise Exception("NoTransform option cannot be combined with other flags.")
        self._config = config
        all_validators = {
            key: self.__get_normalizer(key, orig_validator)
            for key, orig_validator in Draft7Validator.VALIDATORS.items()
            # Do not validate field we do not transform for maximum performance.
            if key in ["type", "array", "$ref", "properties", "items"]
        }
        self._normalizer = validators.create(
            meta_schema=Draft7Validator.META_SCHEMA, validators=all_validators
        )

    def registerCustomTransform(
        self, normalization_callback: Callable[[Any, dict[str, Any]], Any]
    ) -> Callable[[Any, dict[str, Any]], Any]:
        """
        Register custom normalization callback.
        :param normalization_callback function to be used for value
        normalization. Takes original value and part type schema. Should return
        normalized value. See docs/connector-development/cdk-python/schemas.md
        for details.
        :return Same callback, this is useful for using registerCustomTransform function as decorator.
        """
        if TransformConfig.CustomSchemaNormalization not in self._config:
            raise Exception(
                "Please set TransformConfig.CustomSchemaNormalization config before registering custom normalizer"
            )
        self._custom_normalizer = normalization_callback
        return normalization_callback

    def __normalize(self, original_item: Any, subschema: Dict[str, Any]) -> Any:
        """
        Applies different transform function to object's field according to config.
        :param original_item original value of field.
        :param subschema part of the jsonschema containing field type/format data.
        :return Final field value.
        """
        if TransformConfig.DefaultSchemaNormalization in self._config:
            original_item = self.default_convert(original_item, subschema)

        if self._custom_normalizer:
            original_item = self._custom_normalizer(original_item, subschema)
        return original_item

    @staticmethod
    def default_convert(original_item: Any, subschema: Dict[str, Any]) -> Any:
        """
        Default transform function that is used when TransformConfig.DefaultSchemaNormalization flag set.
        :param original_item original value of field.
        :param subschema part of the jsonschema containing field type/format data.
        :return transformed field value.
        """
        target_type = subschema.get("type", [])
        if original_item is None and "null" in target_type:
            return None
        if isinstance(target_type, list):
            # jsonschema type could either be a single string or array of type
            # strings. In case if there is some disambigous and more than one
            # type (except null) do not do any conversion and return original
            # value. If type array has one type and null i.e. {"type":
            # ["integer", "null"]}, convert value to specified type.
            target_type = [t for t in target_type if t != "null"]
            if len(target_type) != 1:
                return original_item
            target_type = target_type[0]
        try:
            if target_type == "string":
                return str(original_item)
            elif target_type == "number":
                return float(original_item)
            elif target_type == "integer":
                return int(original_item)
            elif target_type == "boolean":
                if isinstance(original_item, str):
                    return _strtobool(original_item) == 1
                return bool(original_item)
            elif target_type == "array":
                item_types = set(subschema.get("items", {}).get("type", set()))
                if (
                    item_types.issubset(json_to_python_simple)
                    and type(original_item) in json_to_python_simple.values()
                ):
                    return [original_item]
        except (ValueError, TypeError):
            return original_item
        return original_item

    def __get_normalizer(
        self,
        schema_key: str,
        original_validator: Callable,  # type: ignore[type-arg]
    ) -> Callable[[Any, Any, Any, dict[str, Any]], Generator[Any, Any, None]]:
        """
        Traverse through object fields using native jsonschema validator and apply normalization function.
        :param schema_key related json schema key that currently being validated/normalized.
        :original_validator: native jsonschema validator callback.
        """

        def normalizator(
            validator_instance: Validator,
            property_value: Any,
            instance: Any,
            schema: Dict[str, Any],
        ) -> Generator[Any, Any, None]:
            """
            Jsonschema validator callable it uses for validating instance. We
            override default Draft7Validator to perform value transformation
            before validation take place. We do not take any action except
            logging warn if object does not conform to json schema, just using
            jsonschema algorithm to traverse through object fields.
            Look
            https://python-jsonschema.readthedocs.io/en/stable/creating/?highlight=validators.create#jsonschema.validators.create
            validators parameter for detailed description.
            :
            """

            def resolve(subschema: dict[str, Any]) -> dict[str, Any]:
                if "$ref" in subschema:
                    _, resolved = cast(
                        RefResolver,
                        validator_instance.resolver,
                    ).resolve(subschema["$ref"])
                    return cast(dict[str, Any], resolved)
                return subschema

            # Transform object and array values before running json schema type checking for each element.
            # Recursively normalize every value of the "instance" sub-object,
            # if "instance" is an incorrect type - skip recursive normalization of "instance"
            if schema_key == "properties" and isinstance(instance, dict):
                for k, subschema in property_value.items():
                    if k in instance:
                        subschema = resolve(subschema)
                        instance[k] = self.__normalize(instance[k], subschema)
            # Recursively normalize every item of the "instance" sub-array,
            # if "instance" is an incorrect type - skip recursive normalization of "instance"
            elif schema_key == "items" and isinstance(instance, list):
                subschema = resolve(property_value)
                for index, item in enumerate(instance):
                    instance[index] = self.__normalize(item, subschema)

            # Running native jsonschema traverse algorithm after field normalization is done.
            yield from original_validator(
                validator_instance,
                property_value,
                instance,
                schema,
            )

        return normalizator

    def transform(
        self,
        record: Dict[str, Any],
        schema: Mapping[str, Any],
    ) -> None:
        """
        Normalize and validate according to config.
        :param record: record instance for normalization/transformation. All modification are done by modifying existent object.
        :param schema: object's jsonschema for normalization.
        """
        if TransformConfig.NoTransform in self._config:
            return
        normalizer = self._normalizer(schema)
        for e in normalizer.iter_errors(record):
            """
            just calling normalizer.validate() would throw an exception on
            first validation occurrences and stop processing rest of schema.
            """
            logger.warning(self.get_error_message(e))

    def get_error_message(self, e: ValidationError) -> str:
        """
        Construct a sanitized error message from a ValidationError instance.
        """
        field_path = ".".join(map(str, e.path))
        type_structure = self._get_type_structure(e.instance)

        return f"Failed to transform value from type '{type_structure}' to type '{e.validator_value}' at path: '{field_path}'"

    def _get_type_structure(self, input_data: Any, current_depth: int = 0) -> Any:
        """
        Get the structure of a given input data for use in error message construction.
        """
        # Handle null values
        if input_data is None:
            return "null"

        # Avoid recursing too deep
        if current_depth >= MAX_NESTING_DEPTH:
            return "object" if isinstance(input_data, dict) else python_to_json[type(input_data)]

        if isinstance(input_data, dict):
            return {
                key: self._get_type_structure(field_value, current_depth + 1)
                for key, field_value in input_data.items()
            }

        else:
            return python_to_json[type(input_data)]


class PydanticTypeTransformer:
    """
    A utility class for dynamically generating Pydantic models from JSON schemas
    and transforming records using these models.

    This class allows users to generate a Pydantic model from a given JSON schema
    at runtime, apply optional modifications to the generated model, and use it
    to transform input records by ensuring they conform to the schema.

    Features:
    - Dynamically generates Pydantic models from JSON schemas.
    - Allows users to register a custom model normalizer for modifying the generated models.
    - Provides a caching mechanism for model generation to improve performance.
    - Supports transformation of input records to ensure schema compliance.

    Attributes:
        _custom_model_normalizer (Callable[[Type[BaseModel]], Type[BaseModel]]):
            An optional callback function that allows users to modify the generated model.

    Methods:
        register_custom_model(custom_model_callback):
            Registers a custom callback function to modify the generated model.

        stream_model(json_schema, model_modifier):
            Generates a Pydantic model from a JSON schema and applies an optional model modification function.

        transform(record, schema):
            Transforms a given record using a dynamically generated Pydantic model.
    """

    _custom_model_normalizer: Callable[[Type[BaseModel]], Type[BaseModel]] = None

    def register_custom_model(
        self, custom_model_callback: Callable[[Type[BaseModel]], Type[BaseModel]]
    ) -> Callable[[Type[BaseModel]], Type[BaseModel]]:
        """
        Registers a custom normalization callback to modify the dynamically generated model.

        This function allows users to define a transformation function that modifies
        the generated Pydantic model before it is used for data validation.

        Args:
            custom_model_callback (Callable[[Type[BaseModel]], Type[BaseModel]]):
                A function that takes a generated Pydantic model as input and returns a modified version.

        Returns:
            Callable[[Type[BaseModel]], Type[BaseModel]]: The registered callback function.
        """

        self._custom_model_normalizer = custom_model_callback
        return custom_model_callback

    @lru_cache
    def stream_model(
        self,
        json_schema: str,
        model_modifier: Optional[Callable[[BaseModel], BaseModel]] = None,
    ) -> BaseModel:
        """
        Generates a Pydantic model from a given JSON schema.

        This method dynamically creates a Pydantic model using the provided JSON schema.
        Optionally, a model modification function can be applied to customize the generated model.

        Args:
            json_schema (str): The JSON schema definition as a string.
            model_modifier (Optional[Callable[[Type[BaseModel]], Type[BaseModel]]], optional):
                A function that modifies the generated model before returning it.

        Returns:
            Type[BaseModel]: The generated Pydantic model with optional modifications.
        """
        with TemporaryDirectory() as temporary_directory_name:
            temporary_directory = Path(temporary_directory_name)
            output = Path(temporary_directory / "models.py")
            generate(
                str(json_schema),
                input_file_type=InputFileType.Auto,
                input_filename="example.json",
                output=output,
                class_name="NormalizationModel",
                output_model_type=DataModelType.PydanticV2BaseModel,
            )

            # Load the generated models.py dynamically
            spec = importlib.util.spec_from_file_location("models", output)
            module = importlib.util.module_from_spec(spec)
            sys.modules["models"] = module
            spec.loader.exec_module(module)

            normalization_model = getattr(module, "NormalizationModel")

            # Apply the user-provided modifier
            if model_modifier:
                return model_modifier(normalization_model)

            return normalization_model

    def transform(self, record: Dict[str, Any], schema: Mapping[str, Any]) -> None:
        """
        Transforms a given record using a dynamically generated Pydantic model.

        This method ensures that the input record conforms to the provided schema by:
        - Generating a Pydantic model from the schema.
        - Validating and normalizing the record based on the generated model.
        - Updating the record with the validated and transformed values.

        Args:
            record (Dict[str, Any]): The data record to be transformed.
            schema (Mapping[str, Any]): The schema that defines the expected structure of the record.

        Returns:
            None: The function updates the `record` in place.
        """
        model: BaseModel = self.stream_model(str(schema))
        record.update(model(**record).model_dump())
