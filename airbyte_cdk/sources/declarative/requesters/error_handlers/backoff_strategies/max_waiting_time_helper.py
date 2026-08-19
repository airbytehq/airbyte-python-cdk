#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping, Optional, Union

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils import AirbyteTracedException

MAX_WAITING_TIME_FIELD = "max_waiting_time_in_seconds"


def interpolated_max_waiting_time(
    max_waiting_time_in_seconds: Optional[Union[float, InterpolatedString, str]],
    parameters: Mapping[str, Any],
) -> Optional[InterpolatedString]:
    """
    Cast a `max_waiting_time_in_seconds` field to an InterpolatedString so that a hardcoded number
    and a value interpolated from the config are resolved through the same path.

    :param max_waiting_time_in_seconds: the value as declared on the backoff strategy
    :param parameters: parameters to make available to the interpolation
    :return: the value as an InterpolatedString, or None when no cap is configured
    """
    if max_waiting_time_in_seconds is None or isinstance(
        max_waiting_time_in_seconds, InterpolatedString
    ):
        return max_waiting_time_in_seconds
    return InterpolatedString.create(str(max_waiting_time_in_seconds), parameters=parameters)


def evaluate_max_waiting_time(
    max_waiting_time_in_seconds: Optional[InterpolatedString], config: Config
) -> Optional[float]:
    """
    Resolve a `max_waiting_time_in_seconds` field to a number of seconds.

    The `is None` checks are deliberate: 0 is a meaningful cap -- "never wait" -- so a truthiness
    check would silently disable it.

    A cap is only read while handling an error the requester is already going to retry, so an
    interpolation that cannot be resolved would otherwise surface as an unhandled jinja
    UndefinedError or ValueError in the middle of a sync that has been running fine. It is raised
    as a system error rather than a config error because the field lives in the manifest: whether
    it is a bad expression or a config key the manifest reads but the spec does not expose, the
    connector is at fault and there is nothing for the user to correct.

    :param max_waiting_time_in_seconds: the interpolated field, or None when no cap is configured
    :param config: the connector config to interpolate against
    :return: the cap in seconds, or None when no cap is configured
    """
    if max_waiting_time_in_seconds is None:
        return None
    try:
        evaluated = max_waiting_time_in_seconds.eval(config)
        if evaluated is None or evaluated == "":
            return None
        return float(evaluated)
    except AirbyteTracedException:
        raise
    except Exception as exception:
        raise AirbyteTracedException(
            internal_message=(
                f"Failed to evaluate {MAX_WAITING_TIME_FIELD} "
                f"{max_waiting_time_in_seconds.string!r}: {exception}"
            ),
            message=(
                "The connector could not determine how long it is allowed to wait for a rate "
                "limit to clear. This is a problem with the connector rather than with your "
                "configuration."
            ),
            failure_type=FailureType.system_error,
        ) from exception
