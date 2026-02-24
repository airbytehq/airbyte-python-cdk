#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Mapping, Type, Union

from requests.exceptions import InvalidSchema, InvalidURL, RequestException

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    ResponseAction,
)

DEFAULT_ERROR_MAPPING: Mapping[Union[int, str, Type[Exception]], ErrorResolution] = {
    InvalidSchema: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message="Invalid Protocol Schema: The endpoint that data is being requested from is using an invalid or insecure. Exception: requests.exceptions.InvalidSchema",
    ),
    InvalidURL: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Invalid URL specified or DNS error occurred: The endpoint that data is being requested from is not a valid URL. Exception: requests.exceptions.InvalidURL",
    ),
    RequestException: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="An exception occurred when making the request. Exception: requests.exceptions.RequestException",
    ),
    400: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.system_error,
        error_message="Bad request response from source's API.",
    ),
    401: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message="Authentication failed on source's API. Credentials may be invalid, expired, or lack required access.",
    ),
    403: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message="Source's API denied access. Configured credentials have insufficient permissions.",
    ),
    404: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.system_error,
        error_message="Requested resource not found on source's API.",
    ),
    405: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.system_error,
        error_message="Method not allowed by source's API.",
    ),
    408: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Request to source's API timed out.",
    ),
    429: ErrorResolution(
        response_action=ResponseAction.RATE_LIMITED,
        failure_type=FailureType.transient_error,
        error_message="Rate limit exceeded on source's API.",
    ),
    500: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Internal server error from source's API.",
    ),
    502: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Bad gateway response from source's API.",
    ),
    503: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Source's API is temporarily unavailable.",
    ),
    504: ErrorResolution(
        response_action=ResponseAction.RETRY,
        failure_type=FailureType.transient_error,
        error_message="Gateway timeout from source's API.",
    ),
}
