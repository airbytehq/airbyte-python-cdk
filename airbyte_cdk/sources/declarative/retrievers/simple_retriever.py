#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
from collections import defaultdict
from dataclasses import InitVar, dataclass, field
from functools import partial
from itertools import islice
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import requests
from pydantic import BaseModel
from requests import Response
from typing_extensions import deprecated

from airbyte_cdk.models import AirbyteMessage
from airbyte_cdk.sources.declarative.decoders import JsonDecoder
from airbyte_cdk.sources.declarative.extractors.http_selector import HttpSelector
from airbyte_cdk.sources.declarative.incremental import (
    DatetimeBasedCursor,
    ResumableFullRefreshCursor,
)
from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    CustomIncrementalSync as CustomIncrementalSyncModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    CustomRequester as CustomRequesterModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    DatetimeBasedCursor as DatetimeBasedCursorModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    HttpRequester as HttpRequesterModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    IncrementingCountCursor as IncrementingCountCursorModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    QueryProperties as QueryPropertiesModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    SimpleRetriever as SimpleRetrieverModel,
)
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    SubstreamPartitionRouter as SubstreamPartitionRouterModel,
)
from airbyte_cdk.sources.declarative.parsers.component_constructor import (
    AdditionalFlags,
    ComponentConstructor,
)
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import (
    SinglePartitionRouter,
)
from airbyte_cdk.sources.declarative.requesters.paginators.no_pagination import NoPagination
from airbyte_cdk.sources.declarative.requesters.paginators.paginator import Paginator
from airbyte_cdk.sources.declarative.requesters.query_properties import QueryProperties
from airbyte_cdk.sources.declarative.requesters.request_options import (
    DefaultRequestOptionsProvider,
    RequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.requesters.requester import Requester
from airbyte_cdk.sources.declarative.retrievers.file_uploader import DefaultFileUploader
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.stream_slicers import StreamSlicerTestReadDecorator
from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.http_logger import format_http_message
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.utils.mapping_helpers import combine_mappings
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor


FULL_REFRESH_SYNC_COMPLETE_KEY = "__ab_full_refresh_sync_complete"


@dataclass
class SimpleRetriever(Retriever, ComponentConstructor[SimpleRetrieverModel]):
    """
    Retrieves records by synchronously sending requests to fetch records.

    The retriever acts as an orchestrator between the requester, the record selector, the paginator, and the stream slicer.

    For each stream slice, submit requests until there are no more pages of records to fetch.

    This retriever currently inherits from HttpStream to reuse the request submission and pagination machinery.
    As a result, some of the parameters passed to some methods are unused.
    The two will be decoupled in a future release.

    Attributes:
        stream_name (str): The stream's name
        stream_primary_key (Optional[Union[str, List[str], List[List[str]]]]): The stream's primary key
        requester (Requester): The HTTP requester
        record_selector (HttpSelector): The record selector
        paginator (Optional[Paginator]): The paginator
        stream_slicer (Optional[StreamSlicer]): The stream slicer
        cursor (Optional[cursor]): The cursor
        parameters (Mapping[str, Any]): Additional runtime parameters to be used for string interpolation
    """

    requester: Requester
    record_selector: HttpSelector
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    name: str
    _name: Union[InterpolatedString, str] = field(init=False, repr=False, default="")
    primary_key: Optional[Union[str, List[str], List[List[str]]]]
    _primary_key: str = field(init=False, repr=False, default="")
    paginator: Optional[Paginator] = None
    stream_slicer: StreamSlicer = field(
        default_factory=lambda: SinglePartitionRouter(parameters={})
    )
    request_option_provider: RequestOptionsProvider = field(
        default_factory=lambda: DefaultRequestOptionsProvider(parameters={})
    )
    cursor: Optional[DeclarativeCursor] = None
    ignore_stream_slicer_parameters_on_paginated_requests: bool = False
    additional_query_properties: Optional[QueryProperties] = None
    log_formatter: Optional[Callable[[requests.Response], Any]] = None

    @classmethod
    def _should_use_lazy_simple_retriever(
        cls,
        model: SimpleRetrieverModel,
        additional_flags: AdditionalFlags,
        incremental_sync: Optional[
            Union[
                IncrementingCountCursorModel, DatetimeBasedCursorModel, CustomIncrementalSyncModel
            ]
        ] = None,
        name: Optional[str] = None,
    ) -> bool:
        if name is None:
            raise ValueError(f"name argument is required to instance a {cls.__name__}")
        if (
            model.partition_router
            and isinstance(model.partition_router, SubstreamPartitionRouterModel)
            and not bool(additional_flags.connector_state_manager.get_stream_state(name, None))
            and any(
                parent_stream_config.lazy_read_pointer
                for parent_stream_config in model.partition_router.parent_stream_configs
            )
        ):
            if incremental_sync:
                if incremental_sync.type != "DatetimeBasedCursor":
                    raise ValueError(
                        f"LazySimpleRetriever only supports DatetimeBasedCursor. Found: {incremental_sync.type}."
                    )

                elif incremental_sync.step or incremental_sync.cursor_granularity:
                    raise ValueError(
                        f"Found more that one slice per parent. LazySimpleRetriever only supports single slice read for stream - {name}."
                    )

            if model.decoder and model.decoder.type != "JsonDecoder":
                raise ValueError(
                    f"LazySimpleRetriever only supports JsonDecoder. Found: {model.decoder.type}."
                )
            return True

        return False

    @classmethod
    def resolve_dependencies(
        cls,
        model: SimpleRetrieverModel,
        config: Config,
        dependency_constructor: Callable[..., Any],
        additional_flags: AdditionalFlags,
        *,
        name: Optional[str] = None,
        primary_key: Optional[Union[str, List[str], List[List[str]]]] = None,
        stream_slicer: Optional[StreamSlicer] = None,
        request_options_provider: Optional[RequestOptionsProvider] = None,
        stop_condition_cursor: Optional[Cursor] = None,
        client_side_incremental_sync: Optional[Dict[str, Any]] = None,
        transformations: Optional[List[RecordTransformation]] = None,
        file_uploader: Optional[DefaultFileUploader] = None,
        incremental_sync: Optional[
            Union[
                IncrementingCountCursorModel, DatetimeBasedCursorModel, CustomIncrementalSyncModel
            ]
        ] = None,
        use_cache: Optional[bool] = None,
        log_formatter: Optional[Callable[[Response], Any]] = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        if name is None:
            raise ValueError(f"name argument is required to instance a {cls.__name__}")

        def _get_url() -> str:
            """
            Closure to get the URL from the requester. This is used to get the URL in the case of a lazy retriever.
            This is needed because the URL is not set until the requester is created.
            """

            _url: str = (
                model.requester.url
                if hasattr(model.requester, "url") and model.requester.url is not None
                else requester.get_url()
            )
            _url_base: str = (
                model.requester.url_base
                if hasattr(model.requester, "url_base") and model.requester.url_base is not None
                else requester.get_url_base()
            )

            return _url or _url_base

        def query_properties_in_request_parameters(
            requester: Union[HttpRequesterModel, CustomRequesterModel],
        ) -> bool:
            if not hasattr(requester, "request_parameters"):
                return False
            request_parameters = requester.request_parameters
            if request_parameters and isinstance(request_parameters, Mapping):
                for request_parameter in request_parameters.values():
                    if isinstance(request_parameter, QueryPropertiesModel):
                        return True
            return False

        def _get_log_formatter(
            log_formatter: Callable[[Response], Any] | None,
            name: str,
            additional_flags: AdditionalFlags,
        ) -> Callable[[Response], Any] | None:
            if additional_flags.should_limit_slices_fetched:
                return (
                    (
                        lambda response: format_http_message(
                            response,
                            f"Stream '{name}' request",
                            f"Request performed in order to extract records for stream '{name}'",
                            name,
                        )
                    )
                    if not log_formatter
                    else log_formatter
                )
            return None

        decoder = (
            dependency_constructor(model=model.decoder, config=config)
            if model.decoder
            else JsonDecoder(parameters={})
        )
        record_selector = dependency_constructor(
            model=model.record_selector,
            name=name,
            config=config,
            decoder=decoder,
            transformations=transformations,
            client_side_incremental_sync=client_side_incremental_sync,
            file_uploader=file_uploader,
        )

        query_properties: Optional[QueryProperties] = None
        query_properties_key: Optional[str] = None
        if query_properties_in_request_parameters(model.requester):
            # It is better to be explicit about an error if PropertiesFromEndpoint is defined in multiple
            # places instead of default to request_parameters which isn't clearly documented
            if (
                hasattr(model.requester, "fetch_properties_from_endpoint")
                and model.requester.fetch_properties_from_endpoint
            ):
                raise ValueError(
                    f"PropertiesFromEndpoint should only be specified once per stream, but found in {model.requester.type}.fetch_properties_from_endpoint and {model.requester.type}.request_parameters"
                )

            query_properties_definitions = []
            for key, request_parameter in model.requester.request_parameters.items():  # type: ignore # request_parameters is already validated to be a Mapping using query_properties_in_request_parameters()
                if isinstance(request_parameter, QueryPropertiesModel):
                    query_properties_key = key
                    query_properties_definitions.append(request_parameter)

            if len(query_properties_definitions) > 1:
                raise ValueError(
                    f"request_parameters only supports defining one QueryProperties field, but found {len(query_properties_definitions)} usages"
                )

            if len(query_properties_definitions) == 1:
                query_properties = dependency_constructor(
                    model=query_properties_definitions[0], config=config
                )
        elif (
            hasattr(model.requester, "fetch_properties_from_endpoint")
            and model.requester.fetch_properties_from_endpoint
        ):
            # todo: Deprecate this condition once dependent connectors migrate to query_properties
            query_properties_definition = QueryPropertiesModel(
                type="QueryProperties",
                property_list=model.requester.fetch_properties_from_endpoint,
                always_include_properties=None,
                property_chunking=None,
            )  # type: ignore # $parameters has a default value

            query_properties = dependency_constructor(
                model=query_properties_definition,
                config=config,
                **kwargs,
            )

        elif hasattr(model.requester, "query_properties") and model.requester.query_properties:
            query_properties = dependency_constructor(
                model=model.requester.query_properties,
                config=config,
                **kwargs,
            )

        requester = dependency_constructor(
            model=model.requester,
            decoder=decoder,
            name=name,
            query_properties_key=query_properties_key,
            use_cache=use_cache,
            config=config,
        )

        # Define cursor only if per partition or common incremental support is needed
        cursor = stream_slicer if isinstance(stream_slicer, DeclarativeCursor) else None

        if (
            not isinstance(stream_slicer, DatetimeBasedCursor)
            or type(stream_slicer) is not DatetimeBasedCursor
        ):
            # Many of the custom component implementations of DatetimeBasedCursor override get_request_params() (or other methods).
            # Because we're decoupling RequestOptionsProvider from the Cursor, custom components will eventually need to reimplement
            # their own RequestOptionsProvider. However, right now the existing StreamSlicer/Cursor still can act as the SimpleRetriever's
            # request_options_provider
            request_options_provider = stream_slicer or DefaultRequestOptionsProvider(parameters={})
        elif not request_options_provider:
            request_options_provider = DefaultRequestOptionsProvider(parameters={})

        stream_slicer = stream_slicer or SinglePartitionRouter(parameters={})
        if additional_flags.should_limit_slices_fetched:
            stream_slicer = cast(
                StreamSlicer,
                StreamSlicerTestReadDecorator(
                    wrapped_slicer=stream_slicer,
                    maximum_number_of_slices=additional_flags.limit_slices_fetched or 5,
                ),
            )

        paginator = (
            dependency_constructor(
                model=model.paginator,
                config=config,
                url_base=_get_url(),
                extractor_model=model.record_selector.extractor,
                decoder=decoder,
                cursor_used_for_stop_condition=stop_condition_cursor or None,
            )
            if model.paginator
            else NoPagination(parameters={})
        )

        ignore_stream_slicer_parameters_on_paginated_requests = (
            model.ignore_stream_slicer_parameters_on_paginated_requests or False
        )

        resolved_dependencies = {
            "name": name,
            "paginator": paginator,
            "primary_key": primary_key,
            "requester": requester,
            "record_selector": record_selector,
            "stream_slicer": stream_slicer,
            "request_option_provider": request_options_provider,
            "cursor": cursor,
            "config": config,
            "ignore_stream_slicer_parameters_on_paginated_requests": ignore_stream_slicer_parameters_on_paginated_requests,
            "parameters": model.parameters or {},
        }

        if cls._should_use_lazy_simple_retriever(model, additional_flags, incremental_sync, name):
            return resolved_dependencies

        resolved_dependencies.update(
            {
                "additional_query_properties": query_properties,
                "log_formatter": _get_log_formatter(log_formatter, name, additional_flags),
            }
        )
        return resolved_dependencies

    @classmethod
    def build(
        cls,
        model: SimpleRetrieverModel,
        config: Config,
        dependency_constructor: Callable[..., Any],
        additional_flags: AdditionalFlags,
        **kwargs: Any,
    ) -> "SimpleRetriever":
        """
        Builds up the Component and it's component-specific dependencies.
        Order of operations:
        - build the dependencies first
        - build the component with the resolved dependencies
        """
        resolved_dependencies: Mapping[str, Any] = cls.resolve_dependencies(
            model=model,
            config=config,
            dependency_constructor=dependency_constructor,
            additional_flags=additional_flags,
            **kwargs,
        )
        if cls._should_use_lazy_simple_retriever(
            model=model,
            additional_flags=additional_flags,
            incremental_sync=kwargs.get("incremental_sync"),
            name=kwargs.get("name"),
        ):
            return LazySimpleRetriever(**resolved_dependencies)
        return SimpleRetriever(**resolved_dependencies)

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._paginator = self.paginator or NoPagination(parameters=parameters)
        self._parameters = parameters
        self._name = (
            InterpolatedString(self._name, parameters=parameters)
            if isinstance(self._name, str)
            else self._name
        )

    @property  # type: ignore
    def name(self) -> str:
        """
        :return: Stream name
        """
        return (
            str(self._name.eval(self.config))
            if isinstance(self._name, InterpolatedString)
            else self._name
        )

    @name.setter
    def name(self, value: str) -> None:
        if not isinstance(value, property):
            self._name = value

    def _get_mapping(
        self, method: Callable[..., Optional[Union[Mapping[str, Any], str]]], **kwargs: Any
    ) -> Tuple[Union[Mapping[str, Any], str], Set[str]]:
        """
        Get mapping from the provided method, and get the keys of the mapping.
        If the method returns a string, it will return the string and an empty set.
        If the method returns a dict, it will return the dict and its keys.
        """
        mapping = method(**kwargs) or {}
        keys = set(mapping.keys()) if not isinstance(mapping, str) else set()
        return mapping, keys

    def _get_request_options(
        self,
        stream_state: Optional[StreamData],
        stream_slice: Optional[StreamSlice],
        next_page_token: Optional[Mapping[str, Any]],
        paginator_method: Callable[..., Optional[Union[Mapping[str, Any], str]]],
        stream_slicer_method: Callable[..., Optional[Union[Mapping[str, Any], str]]],
    ) -> Union[Mapping[str, Any], str]:
        """
        Get the request_option from the paginator and the stream slicer.
        Raise a ValueError if there's a key collision
        Returned merged mapping otherwise
        """
        # FIXME we should eventually remove the usage of stream_state as part of the interpolation

        is_body_json = paginator_method.__name__ == "get_request_body_json"

        mappings = [
            paginator_method(
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
        ]
        if not next_page_token or not self.ignore_stream_slicer_parameters_on_paginated_requests:
            mappings.append(
                stream_slicer_method(
                    stream_slice=stream_slice,
                    next_page_token=next_page_token,
                )
            )
        return combine_mappings(mappings, allow_same_value_merge=is_body_json)

    def _request_headers(
        self,
        stream_state: Optional[StreamData] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        Specifies request headers.
        Authentication headers will overwrite any overlapping headers returned from this method.
        """
        headers = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self._paginator.get_request_headers,
            self.request_option_provider.get_request_headers,
        )
        if isinstance(headers, str):
            raise ValueError("Request headers cannot be a string")
        return {str(k): str(v) for k, v in headers.items()}

    def _request_params(
        self,
        stream_state: Optional[StreamData] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        Specifies the query parameters that should be set on an outgoing HTTP request given the inputs.

        E.g: you might want to define query parameters for paging if next_page_token is not None.
        """
        params = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self._paginator.get_request_params,
            self.request_option_provider.get_request_params,
        )
        if isinstance(params, str):
            raise ValueError("Request params cannot be a string")
        return params

    def _request_body_data(
        self,
        stream_state: Optional[StreamData] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        """
        Specifies how to populate the body of the request with a non-JSON payload.

        If returns a ready text that it will be sent as is.
        If returns a dict that it will be converted to a urlencoded form.
        E.g. {"key1": "value1", "key2": "value2"} => "key1=value1&key2=value2"

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        return self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self._paginator.get_request_body_data,
            self.request_option_provider.get_request_body_data,
        )

    def _request_body_json(
        self,
        stream_state: Optional[StreamData] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Specifies how to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        body_json = self._get_request_options(
            stream_state,
            stream_slice,
            next_page_token,
            self._paginator.get_request_body_json,
            self.request_option_provider.get_request_body_json,
        )
        if isinstance(body_json, str):
            raise ValueError("Request body json cannot be a string")
        return body_json

    def _paginator_path(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Optional[str]:
        """
        If the paginator points to a path, follow it, else return nothing so the requester is used.
        :param next_page_token:
        :return:
        """
        return self._paginator.path(
            next_page_token=next_page_token,
            stream_state=stream_state,
            stream_slice=stream_slice,
        )

    def _parse_response(
        self,
        response: Optional[requests.Response],
        stream_state: StreamState,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Record]:
        if not response:
            yield from []
        else:
            yield from self.record_selector.select_records(
                response=response,
                stream_state=stream_state,
                records_schema=records_schema,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            )

    @property  # type: ignore
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """The stream's primary key"""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: str) -> None:
        if not isinstance(value, property):
            self._primary_key = value

    def _next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any],
    ) -> Optional[Mapping[str, Any]]:
        """
        Specifies a pagination strategy.

        The value returned from this method is passed to most other methods in this class. Use it to form a request e.g: set headers or query params.

        :return: The token for the next page from the input response object. Returning None means there are no more pages to read in this response.
        """
        return self._paginator.next_page_token(
            response=response,
            last_page_size=last_page_size,
            last_record=last_record,
            last_page_token_value=last_page_token_value,
        )

    def _fetch_next_page(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[requests.Response]:
        return self.requester.send_request(
            path=self._paginator_path(
                next_page_token=next_page_token,
                stream_state=stream_state,
                stream_slice=stream_slice,
            ),
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
            request_headers=self._request_headers(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            request_params=self._request_params(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            request_body_data=self._request_body_data(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            request_body_json=self._request_body_json(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
            log_formatter=self.log_formatter,
        )

    # This logic is similar to _read_pages in the HttpStream class. When making changes here, consider making changes there as well.
    def _read_pages(
        self,
        records_generator_fn: Callable[[Optional[requests.Response]], Iterable[Record]],
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
    ) -> Iterable[Record]:
        pagination_complete = False
        initial_token = self._paginator.get_initial_token()
        next_page_token: Optional[Mapping[str, Any]] = (
            {"next_page_token": initial_token} if initial_token is not None else None
        )
        while not pagination_complete:
            property_chunks: List[List[str]] = (
                list(
                    self.additional_query_properties.get_request_property_chunks(
                        stream_slice=stream_slice
                    )
                )
                if self.additional_query_properties
                else [
                    []
                ]  # A single empty property chunk represents the case where property chunking is not configured
            )

            merged_records: MutableMapping[str, Any] = defaultdict(dict)
            last_page_size = 0
            last_record: Optional[Record] = None
            response: Optional[requests.Response] = None
            for properties in property_chunks:
                if len(properties) > 0:
                    stream_slice = StreamSlice(
                        partition=stream_slice.partition or {},
                        cursor_slice=stream_slice.cursor_slice or {},
                        extra_fields={"query_properties": properties},
                    )

                response = self._fetch_next_page(stream_state, stream_slice, next_page_token)
                for current_record in records_generator_fn(response):
                    if (
                        current_record
                        and self.additional_query_properties
                        and self.additional_query_properties.property_chunking
                    ):
                        merge_key = (
                            self.additional_query_properties.property_chunking.get_merge_key(
                                current_record
                            )
                        )
                        if merge_key:
                            _deep_merge(merged_records[merge_key], current_record)
                        else:
                            # We should still emit records even if the record did not have a merge key
                            last_page_size += 1
                            last_record = current_record
                            yield current_record
                    else:
                        last_page_size += 1
                        last_record = current_record
                        yield current_record

            if (
                self.additional_query_properties
                and self.additional_query_properties.property_chunking
            ):
                for merged_record in merged_records.values():
                    record = Record(
                        data=merged_record, stream_name=self.name, associated_slice=stream_slice
                    )
                    last_page_size += 1
                    last_record = record
                    yield record

            if not response:
                pagination_complete = True
            else:
                last_page_token_value = (
                    next_page_token.get("next_page_token") if next_page_token else None
                )
                next_page_token = self._next_page_token(
                    response=response,
                    last_page_size=last_page_size,
                    last_record=last_record,
                    last_page_token_value=last_page_token_value,
                )
                if not next_page_token:
                    pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _read_single_page(
        self,
        records_generator_fn: Callable[[Optional[requests.Response]], Iterable[Record]],
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
    ) -> Iterable[StreamData]:
        initial_token = stream_state.get("next_page_token")
        if initial_token is None:
            initial_token = self._paginator.get_initial_token()
        next_page_token: Optional[Mapping[str, Any]] = (
            {"next_page_token": initial_token} if initial_token else None
        )

        response = self._fetch_next_page(stream_state, stream_slice, next_page_token)

        last_page_size = 0
        last_record: Optional[Record] = None
        for record in records_generator_fn(response):
            last_page_size += 1
            last_record = record
            yield record

        if not response:
            next_page_token = {FULL_REFRESH_SYNC_COMPLETE_KEY: True}
        else:
            last_page_token_value = (
                next_page_token.get("next_page_token") if next_page_token else None
            )
            next_page_token = self._next_page_token(
                response=response,
                last_page_size=last_page_size,
                last_record=last_record,
                last_page_token_value=last_page_token_value,
            ) or {FULL_REFRESH_SYNC_COMPLETE_KEY: True}

        if self.cursor:
            self.cursor.close_slice(
                StreamSlice(cursor_slice=next_page_token, partition=stream_slice.partition)
            )

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        """
        Fetch a stream's records from an HTTP API source

        :param records_schema: json schema to describe record
        :param stream_slice: The stream slice to read data for
        :return: The records read from the API source
        """
        _slice = stream_slice or StreamSlice(partition={}, cursor_slice={})  # None-check

        most_recent_record_from_slice = None
        record_generator = partial(
            self._parse_records,
            stream_slice=stream_slice,
            stream_state=self.state or {},
            records_schema=records_schema,
        )

        if self.cursor and isinstance(self.cursor, ResumableFullRefreshCursor):
            stream_state = self.state

            # Before syncing the RFR stream, we check if the job's prior attempt was successful and don't need to
            # fetch more records. The platform deletes stream state for full refresh streams before starting a
            # new job, so we don't need to worry about this value existing for the initial attempt
            if stream_state.get(FULL_REFRESH_SYNC_COMPLETE_KEY):
                return

            yield from self._read_single_page(record_generator, stream_state, _slice)
        else:
            for stream_data in self._read_pages(record_generator, self.state, _slice):
                current_record = self._extract_record(stream_data, _slice)
                if self.cursor and current_record:
                    self.cursor.observe(_slice, current_record)

                yield stream_data

            if self.cursor:
                self.cursor.close_slice(_slice)
        return

    # FIXME based on the comment above in SimpleRetriever.read_records, it seems like we can tackle https://github.com/airbytehq/airbyte-internal-issues/issues/6955 and remove this

    def _extract_record(
        self, stream_data: StreamData, stream_slice: StreamSlice
    ) -> Optional[Record]:
        """
        As we allow the output of _read_pages to be StreamData, it can be multiple things. Therefore, we need to filter out and normalize
        to data to streamline the rest of the process.
        """
        if isinstance(stream_data, Record):
            # Record is not part of `StreamData` but is the most common implementation of `Mapping[str, Any]` which is part of `StreamData`
            return stream_data
        elif isinstance(stream_data, (dict, Mapping)):
            return Record(
                data=dict(stream_data), associated_slice=stream_slice, stream_name=self.name
            )
        elif isinstance(stream_data, AirbyteMessage) and stream_data.record:
            return Record(
                data=stream_data.record.data,  # type:ignore # AirbyteMessage always has record.data
                associated_slice=stream_slice,
                stream_name=self.name,
            )
        return None

    # stream_slices is defined with arguments on http stream and fixing this has a long tail of dependencies. Will be resolved by the decoupling of http stream and simple retriever
    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:  # type: ignore
        """
        Specifies the slices for this stream. See the stream slicing section of the docs for more information.

        :param sync_mode:
        :param cursor_field:
        :param stream_state:
        :return:
        """
        return self.stream_slicer.stream_slices()

    @property
    def state(self) -> Mapping[str, Any]:
        return self.cursor.get_stream_state() if self.cursor else {}

    @state.setter
    def state(self, value: StreamState) -> None:
        """State setter, accept state serialized by state getter."""
        if self.cursor:
            self.cursor.set_initial_state(value)

    def _parse_records(
        self,
        response: Optional[requests.Response],
        stream_state: Mapping[str, Any],
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice],
    ) -> Iterable[Record]:
        yield from self._parse_response(
            response,
            stream_slice=stream_slice,
            stream_state=stream_state,
            records_schema=records_schema,
        )

    def must_deduplicate_query_params(self) -> bool:
        return True

    @staticmethod
    def _to_partition_key(to_serialize: Any) -> str:
        # separators have changed in Python 3.4. To avoid being impacted by further change, we explicitly specify our own value
        return json.dumps(to_serialize, indent=None, separators=(",", ":"), sort_keys=True)


def _deep_merge(
    target: MutableMapping[str, Any], source: Union[Record, MutableMapping[str, Any]]
) -> None:
    """
    Recursively merge two dictionaries, combining nested dictionaries instead of overwriting them.

    :param target: The dictionary to merge into (modified in place)
    :param source: The dictionary to merge from
    """
    for key, value in source.items():
        if (
            key in target
            and isinstance(target[key], MutableMapping)
            and isinstance(value, MutableMapping)
        ):
            _deep_merge(target[key], value)
        else:
            target[key] = value


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
@dataclass
class LazySimpleRetriever(SimpleRetriever):
    """
    A retriever that supports lazy loading from parent streams.
    """

    def _read_pages(
        self,
        records_generator_fn: Callable[[Optional[requests.Response]], Iterable[Record]],
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
    ) -> Iterable[Record]:
        response = stream_slice.extra_fields["child_response"]
        if response:
            last_page_size, last_record = 0, None
            for record in records_generator_fn(response):  # type: ignore[call-arg] # only _parse_records expected as a func
                last_page_size += 1
                last_record = record
                yield record

            next_page_token = self._next_page_token(response, last_page_size, last_record, None)
            if next_page_token:
                yield from self._paginate(
                    next_page_token,
                    records_generator_fn,
                    stream_state,
                    stream_slice,
                )

            yield from []
        else:
            yield from self._read_pages(records_generator_fn, stream_state, stream_slice)

    def _paginate(
        self,
        next_page_token: Any,
        records_generator_fn: Callable[[Optional[requests.Response]], Iterable[Record]],
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
    ) -> Iterable[Record]:
        """Handle pagination by fetching subsequent pages."""
        pagination_complete = False

        while not pagination_complete:
            response = self._fetch_next_page(stream_state, stream_slice, next_page_token)
            last_page_size, last_record = 0, None

            for record in records_generator_fn(response):  # type: ignore[call-arg] # only _parse_records expected as a func
                last_page_size += 1
                last_record = record
                yield record

            if not response:
                pagination_complete = True
            else:
                last_page_token_value = (
                    next_page_token.get("next_page_token") if next_page_token else None
                )
                next_page_token = self._next_page_token(
                    response, last_page_size, last_record, last_page_token_value
                )

                if not next_page_token:
                    pagination_complete = True
