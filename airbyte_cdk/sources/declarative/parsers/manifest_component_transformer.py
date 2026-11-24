#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import typing
from typing import Any, Dict, Mapping, Optional

PARAMETERS_STR = "$parameters"
# Inside a `STREAM_INDEPENDENT_FIELDS` subtree: the `$parameters` the component declares itself,
# kept apart from the outer ones merged into its `$parameters`. A second propagation (dynamic
# streams are propagated again once resolved) reads it to tell the two apart.
OWN_PARAMETERS_STR = "$own_parameters"


DEFAULT_MODEL_TYPES: Mapping[str, str] = {
    # CompositeErrorHandler
    "CompositeErrorHandler.error_handlers": "DefaultErrorHandler",
    # CursorPagination
    "CursorPagination.decoder": "JsonDecoder",
    # DatetimeBasedCursor
    "DatetimeBasedCursor.end_datetime": "MinMaxDatetime",
    "DatetimeBasedCursor.end_time_option": "RequestOption",
    "DatetimeBasedCursor.start_datetime": "MinMaxDatetime",
    "DatetimeBasedCursor.start_time_option": "RequestOption",
    # DeclarativeSource
    "DeclarativeSource.check": "CheckStream",
    "DeclarativeSource.spec": "Spec",
    "DeclarativeSource.streams": "DeclarativeStream",
    # DeclarativeStream
    "DeclarativeStream.retriever": "SimpleRetriever",
    "DeclarativeStream.schema_loader": "JsonFileSchemaLoader",
    # DynamicDeclarativeStream
    "DynamicDeclarativeStream.stream_template": "DeclarativeStream",
    "DynamicDeclarativeStream.components_resolver": "ConfigComponentResolver",
    # HttpComponentsResolver
    "HttpComponentsResolver.retriever": "SimpleRetriever",
    "HttpComponentsResolver.components_mapping": "ComponentMappingDefinition",
    # ConfigComponentResolver
    "ConfigComponentsResolver.stream_config": "StreamConfig",
    "ConfigComponentsResolver.components_mapping": "ComponentMappingDefinition",
    # DefaultErrorHandler
    "DefaultErrorHandler.response_filters": "HttpResponseFilter",
    # DefaultPaginator
    "DefaultPaginator.decoder": "JsonDecoder",
    "DefaultPaginator.page_size_option": "RequestOption",
    # DpathExtractor
    "DpathExtractor.decoder": "JsonDecoder",
    "DpathExtractor.record_expander": "RecordExpander",
    # HttpRequester
    "HttpRequester.error_handler": "DefaultErrorHandler",
    # ListPartitionRouter
    "ListPartitionRouter.request_option": "RequestOption",
    # ParentStreamConfig
    "ParentStreamConfig.request_option": "RequestOption",
    "ParentStreamConfig.stream": "DeclarativeStream",
    # RecordExpander
    "RecordExpander.parent_fields": "ParentFieldPath",
    # RecordSelector
    "RecordSelector.extractor": "DpathExtractor",
    "RecordSelector.record_filter": "RecordFilter",
    # SimpleRetriever
    "SimpleRetriever.paginator": "NoPagination",
    "SimpleRetriever.record_selector": "RecordSelector",
    "SimpleRetriever.requester": "HttpRequester",
    # SubstreamPartitionRouter
    "SubstreamPartitionRouter.parent_stream_configs": "ParentStreamConfig",
    # AddFields
    "AddFields.fields": "AddedFieldDefinition",
    # CustomPartitionRouter
    "CustomPartitionRouter.parent_stream_configs": "ParentStreamConfig",
    # DynamicSchemaLoader
    "DynamicSchemaLoader.retriever": "SimpleRetriever",
    # SchemaTypeIdentifier
    "SchemaTypeIdentifier.types_map": "TypesMap",
}

# Fields whose subtree is shared by every stream, so the `$parameters` of the streams around it
# must not fill its fields: each stream would otherwise see a different definition (a stream's
# `url` filling the login's empty `url`), and the factory could no longer share one instance.
# The outer parameters are still passed down as `$parameters`, so a component inside the subtree
# can read them, and `$parameters` declared inside the subtree fill its fields as usual.
STREAM_INDEPENDENT_FIELDS = frozenset({"CredentialSet.authenticator"})

# We retain a separate registry for custom components to automatically insert the type if it is missing. This is intended to
# be a short term fix because once we have migrated, then type and class_name should be requirements for all custom components.
CUSTOM_COMPONENTS_MAPPING: Mapping[str, str] = {
    "CompositeErrorHandler.backoff_strategies": "CustomBackoffStrategy",
    "DeclarativeStream.retriever": "CustomRetriever",
    "DeclarativeStream.transformations": "CustomTransformation",
    "DefaultErrorHandler.backoff_strategies": "CustomBackoffStrategy",
    "DefaultPaginator.pagination_strategy": "CustomPaginationStrategy",
    "HttpRequester.authenticator": "CustomAuthenticator",
    "HttpRequester.error_handler": "CustomErrorHandler",
    "RecordSelector.extractor": "CustomRecordExtractor",
    "SimpleRetriever.partition_router": "CustomPartitionRouter",
}


class ManifestComponentTransformer:
    def propagate_types_and_parameters(
        self,
        parent_field_identifier: str,
        declarative_component: Mapping[str, Any],
        parent_parameters: Mapping[str, Any],
        use_parent_parameters: Optional[bool] = None,
        parent_field_parameters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Recursively transforms the specified declarative component and subcomponents to propagate parameters and insert the
        default component type if it was not already present. The resulting transformed components are a deep copy of the input
        components, not an in-place transformation.

        :param declarative_component: The current component that is having type and parameters added
        :param parent_field_identifier: The name of the field of the current component coming from the parent component
        :param parent_parameters: The parameters set on parent components defined before the current component
        :param use_parent_parameters: If set, parent parameters will be used as the source of truth when key names are the same
        :param parent_field_parameters: The parent parameters that may fill the current component's fields, when only some of
            `parent_parameters` may (see `STREAM_INDEPENDENT_FIELDS`). `None` means all of them.
        :return: A deep copy of the transformed component with types and parameters persisted to it
        """
        propagated_component = dict(copy.deepcopy(declarative_component))
        if "type" not in propagated_component:
            # If the component has class_name we assume that this is a reference to a custom component. This is a slight change to
            # existing behavior because we originally allowed for either class or type to be specified. After the pydantic migration,
            # class_name will only be a valid field on custom components and this change reflects that. I checked, and we currently
            # have no low-code connectors that use class_name except for custom components.
            if "class_name" in propagated_component:
                found_type = CUSTOM_COMPONENTS_MAPPING.get(parent_field_identifier)
            else:
                found_type = DEFAULT_MODEL_TYPES.get(parent_field_identifier)
            if found_type:
                propagated_component["type"] = found_type

        # Combines parameters defined at the current level with parameters from parent components. Parameters at the current
        # level take precedence
        current_parameters = dict(copy.deepcopy(parent_parameters))
        component_parameters = propagated_component.pop(PARAMETERS_STR, {})
        recorded_own_parameters = propagated_component.pop(OWN_PARAMETERS_STR, None)
        current_parameters = self._merge_parameters(
            current_parameters, component_parameters, use_parent_parameters
        )
        # The parameters that fill this component's fields. Usually the same dict as
        # `current_parameters`, so the exclusions below apply to both.
        own_parameters: Optional[Dict[str, Any]] = None
        if parent_field_parameters is None:
            field_parameters = current_parameters
        else:
            # On a second propagation `$parameters` already holds the outer parameters merged
            # in by the first one, so only the recorded own ones may fill fields.
            own_parameters = dict(
                component_parameters if recorded_own_parameters is None else recorded_own_parameters
            )
            field_parameters = self._merge_parameters(
                dict(copy.deepcopy(parent_field_parameters)),
                own_parameters,
                use_parent_parameters,
            )

        # When there is no resolved type, we're not processing a component (likely a regular object) and don't need to propagate parameters
        # When the type refers to a json schema, we're not processing a component as well. This check is currently imperfect as there could
        # be json_schema are not objects but we believe this is not likely in our case because:
        # * records are Mapping so objects hence SchemaLoader root should be an object
        # * connection_specification is a Mapping
        if self._is_json_schema_object(propagated_component):
            return propagated_component

        # For objects that don't have type check if their object fields have nested components which should have `$parameters` in it.
        # For example, QueryProperties in requester.request_parameters, etc.
        # Update propagated_component value with nested components with parent `$parameters` if needed and return propagated_component.
        if "type" not in propagated_component:
            if self._has_nested_components(propagated_component):
                propagated_component = self._process_nested_components(
                    propagated_component,
                    parent_field_identifier,
                    current_parameters,
                    use_parent_parameters,
                    self._child_field_parameters(
                        parent_field_identifier, current_parameters, field_parameters
                    ),
                )
            return propagated_component

        # Parameters should be applied to the current component fields with the existing field taking precedence over parameters if
        # both exist
        for parameter_key, parameter_value in field_parameters.items():
            propagated_component[parameter_key] = (
                propagated_component.get(parameter_key) or parameter_value
            )

        for field_name, field_value in propagated_component.items():
            if isinstance(field_value, dict):
                # We exclude propagating a parameter that matches the current field name because that would result in an infinite cycle
                excluded_parameter = current_parameters.pop(field_name, None)
                excluded_field_parameter = field_parameters.pop(field_name, None)
                parent_type_field_identifier = f"{propagated_component.get('type')}.{field_name}"
                propagated_component[field_name] = self.propagate_types_and_parameters(
                    parent_type_field_identifier,
                    field_value,
                    current_parameters,
                    use_parent_parameters=use_parent_parameters,
                    parent_field_parameters=self._child_field_parameters(
                        parent_type_field_identifier, current_parameters, field_parameters
                    ),
                )
                if excluded_parameter:
                    current_parameters[field_name] = excluded_parameter
                if excluded_field_parameter:
                    field_parameters[field_name] = excluded_field_parameter
            elif isinstance(field_value, typing.List):
                # We exclude propagating a parameter that matches the current field name because that would result in an infinite cycle
                excluded_parameter = current_parameters.pop(field_name, None)
                excluded_field_parameter = field_parameters.pop(field_name, None)
                for i, element in enumerate(field_value):
                    if isinstance(element, dict):
                        parent_type_field_identifier = (
                            f"{propagated_component.get('type')}.{field_name}"
                        )
                        field_value[i] = self.propagate_types_and_parameters(
                            parent_type_field_identifier,
                            element,
                            current_parameters,
                            use_parent_parameters=use_parent_parameters,
                            parent_field_parameters=self._child_field_parameters(
                                parent_type_field_identifier, current_parameters, field_parameters
                            ),
                        )
                if excluded_parameter:
                    current_parameters[field_name] = excluded_parameter
                if excluded_field_parameter:
                    field_parameters[field_name] = excluded_field_parameter

        if current_parameters:
            propagated_component[PARAMETERS_STR] = current_parameters
        if own_parameters is not None:
            propagated_component[OWN_PARAMETERS_STR] = own_parameters
        return propagated_component

    @staticmethod
    def _merge_parameters(
        parent_parameters: Dict[str, Any],
        component_parameters: Mapping[str, Any],
        use_parent_parameters: Optional[bool],
    ) -> Dict[str, Any]:
        return (
            {**component_parameters, **parent_parameters}
            if use_parent_parameters
            else {**parent_parameters, **component_parameters}
        )

    @staticmethod
    def _child_field_parameters(
        child_field_identifier: str,
        current_parameters: Dict[str, Any],
        field_parameters: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """The `parent_field_parameters` to pass to a child: `None` while they are all parameters."""
        if child_field_identifier in STREAM_INDEPENDENT_FIELDS:
            return {}
        return None if field_parameters is current_parameters else field_parameters

    @staticmethod
    def _is_json_schema_object(propagated_component: Mapping[str, Any]) -> bool:
        # Component types are always strings: a list is a JSON schema type union and a dict is a
        # `properties` map that has a property named `type`.
        component_type = propagated_component.get("type")
        return component_type == "object" or isinstance(component_type, (list, dict))

    @staticmethod
    def _has_nested_components(propagated_component: Dict[str, Any]) -> bool:
        for k, v in propagated_component.items():
            if isinstance(v, dict) and v.get("type"):
                return True
        return False

    def _process_nested_components(
        self,
        propagated_component: Dict[str, Any],
        parent_field_identifier: str,
        current_parameters: Mapping[str, Any],
        use_parent_parameters: Optional[bool] = None,
        field_parameters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        for field_name, field_value in propagated_component.items():
            if isinstance(field_value, dict) and field_value.get("type"):
                nested_component_with_parameters = self.propagate_types_and_parameters(
                    parent_field_identifier,
                    field_value,
                    current_parameters,
                    use_parent_parameters=use_parent_parameters,
                    parent_field_parameters=field_parameters,
                )
                propagated_component[field_name] = nested_component_with_parameters

        return propagated_component
