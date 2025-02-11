#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Dict, Iterator, List, Mapping, Optional

from airbyte_cdk.connector_builder.models import (
    HttpRequest,
    HttpResponse,
    StreamReadPages,
    StreamReadSlices,
)
from airbyte_cdk.models import (
    AirbyteMessage,
)
from airbyte_cdk.utils.datetime_format_inferrer import DatetimeFormatInferrer
from airbyte_cdk.utils.schema_inferrer import (
    SchemaInferrer,
)

from .helpers import (
    _airbyte_message_to_json,
    _close_current_page,
    _handle_log_message,
    _handle_record_message,
    _is_config_update_message,
    _is_log_message,
    _is_record_message,
    _is_state_message,
    _is_trace_with_error,
    _need_to_close_page,
    _need_to_close_page_in_slice,
    _need_to_process_slice_descriptor,
    _parse_slice_description,
)
from .types import MESSAGE_GROUPS


def get_message_groups(
    messages: Iterator[AirbyteMessage],
    schema_inferrer: SchemaInferrer,
    datetime_format_inferrer: DatetimeFormatInferrer,
    limit: int,
) -> MESSAGE_GROUPS:
    """
    Processes an iterator of AirbyteMessage objects to group and yield messages based on their type and sequence.

    This function iterates over the provided messages until the number of record messages processed reaches the specified limit.
    It accumulates messages into pages and slices, handling various types of messages such as log, trace (with errors), record,
    configuration update, and state messages. The function makes use of helper routines to:
        - Convert messages to JSON.
        - Determine when to close a page or a slice.
        - Parse slice descriptors.
        - Handle log messages and auxiliary requests.
        - Process record messages while inferring schema and datetime formats.

    Depending on the incoming message type, it may yield:
        - StreamReadSlices objects when a slice is completed.
        - Auxiliary HTTP requests/responses generated from log messages.
        - Error trace messages if encountered.
        - Configuration update messages.

    Parameters:
        messages (Iterator[AirbyteMessage]): An iterator yielding AirbyteMessage instances.
        schema_inferrer (SchemaInferrer): An instance used to infer and update schema based on record messages.
        datetime_format_inferrer (DatetimeFormatInferrer): An instance used to infer datetime formats from record messages.
        limit (int): The maximum number of record messages to process before stopping.

    Yields:
        Depending on the type of message processed, one or more of the following:
            - StreamReadSlices: A grouping of pages within a slice along with the slice descriptor and state.
            - HttpRequest/HttpResponse: Auxiliary request/response information derived from log messages.
            - TraceMessage: Error details when a trace message with errors is encountered.
            - ControlMessage: Configuration update details.

    Notes:
        The function depends on several helper functions (e.g., _airbyte_message_to_json, _need_to_close_page,
        _close_current_page, _parse_slice_description, _handle_log_message, and others) to encapsulate specific behavior.
        It maintains internal state for grouping pages and slices, ensuring that related messages are correctly aggregated
        and yielded as complete units.
    """

    records_count = 0
    at_least_one_page_in_group = False
    current_page_records: List[Mapping[str, Any]] = []
    current_slice_descriptor: Optional[Dict[str, Any]] = None
    current_slice_pages: List[StreamReadPages] = []
    current_page_request: Optional[HttpRequest] = None
    current_page_response: Optional[HttpResponse] = None
    latest_state_message: Optional[Dict[str, Any]] = None

    def _processed_slice() -> StreamReadSlices:
        """
        The closure, to create a `StreamReadSlices` object with the current slice pages, slice descriptor, and state.

        Returns:
            StreamReadSlices: An object containing the current slice pages, slice descriptor, and state.
        """
        return StreamReadSlices(
            pages=current_slice_pages,
            slice_descriptor=current_slice_descriptor,
            state=[latest_state_message] if latest_state_message else [],
        )

    while records_count < limit and (message := next(messages, None)):
        json_message = _airbyte_message_to_json(message)

        if _need_to_close_page(at_least_one_page_in_group, message, json_message):
            current_page_request, current_page_response = _close_current_page(
                current_page_request,
                current_page_response,
                current_slice_pages,
                current_page_records,
            )

        if _need_to_close_page_in_slice(at_least_one_page_in_group, message):
            yield _processed_slice()
            current_slice_descriptor = _parse_slice_description(message.log.message)  # type: ignore
            current_slice_pages = []
            at_least_one_page_in_group = False
        elif _need_to_process_slice_descriptor(message):
            # parsing the first slice
            current_slice_descriptor = _parse_slice_description(message.log.message)  # type: ignore
        elif _is_log_message(message):
            (
                at_least_one_page_in_group,
                current_page_request,
                current_page_response,
                log_or_auxiliary_request,
            ) = _handle_log_message(
                message,
                json_message,
                at_least_one_page_in_group,
                current_page_request,
                current_page_response,
            )
            if log_or_auxiliary_request:
                yield log_or_auxiliary_request
        elif _is_trace_with_error(message):
            if message.trace is not None:
                yield message.trace
        elif _is_record_message(message):
            records_count = _handle_record_message(
                message,
                schema_inferrer,
                datetime_format_inferrer,
                records_count,
                current_page_records,
            )
        elif _is_config_update_message(message):
            if message.control is not None:
                yield message.control
        elif _is_state_message(message):
            latest_state_message = message.state  # type: ignore

    else:
        if current_page_request or current_page_response or current_page_records:
            _close_current_page(
                current_page_request,
                current_page_response,
                current_slice_pages,
                current_page_records,
            )
            yield _processed_slice()
