#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import orjson
import pytest

from airbyte_cdk.models import (
    AirbyteErrorTraceMessage,
    AirbyteMessage,
    AirbyteMessageSerializer,
    AirbyteTraceMessage,
    FailureType,
    Status,
    StreamDescriptor,
    TraceType,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_AN_EXCEPTION = ValueError("An exception")
_A_STREAM_DESCRIPTOR = StreamDescriptor(name="a_stream")
_ANOTHER_STREAM_DESCRIPTOR = StreamDescriptor(name="another_stream")


@pytest.fixture
def raised_exception():
    try:
        raise RuntimeError("an error has occurred")
    except RuntimeError as e:
        return e


def test_build_from_existing_exception(raised_exception):
    traced_exc = AirbyteTracedException.from_exception(
        raised_exception, message="my user-friendly message"
    )
    assert traced_exc.message == "my user-friendly message"
    assert traced_exc.internal_message == "an error has occurred"
    assert traced_exc.failure_type == FailureType.system_error
    assert traced_exc._exception == raised_exception


def test_exception_as_airbyte_message():
    traced_exc = AirbyteTracedException("an internal message")
    airbyte_message = traced_exc.as_airbyte_message()

    assert isinstance(airbyte_message, AirbyteMessage)
    assert airbyte_message.type == MessageType.TRACE
    assert airbyte_message.trace.type == TraceType.ERROR
    assert airbyte_message.trace.emitted_at > 0
    assert airbyte_message.trace.error.failure_type == FailureType.system_error
    assert (
        airbyte_message.trace.error.message
        == "Something went wrong in the connector. See the logs for more details."
    )
    assert airbyte_message.trace.error.internal_message == "an internal message"
    assert (
        airbyte_message.trace.error.stack_trace
        == "airbyte_cdk.utils.traced_exception.AirbyteTracedException: an internal message\n"
    )


def test_existing_exception_as_airbyte_message(raised_exception):
    traced_exc = AirbyteTracedException.from_exception(raised_exception)
    airbyte_message = traced_exc.as_airbyte_message()

    assert isinstance(airbyte_message, AirbyteMessage)
    assert airbyte_message.type == MessageType.TRACE
    assert airbyte_message.trace.type == TraceType.ERROR
    assert (
        airbyte_message.trace.error.message
        == "Something went wrong in the connector. See the logs for more details."
    )
    assert airbyte_message.trace.error.internal_message == "an error has occurred"
    assert airbyte_message.trace.error.stack_trace.startswith("Traceback (most recent call last):")
    assert airbyte_message.trace.error.stack_trace.endswith(
        'raise RuntimeError("an error has occurred")\nRuntimeError: an error has occurred\n'
    )


def test_config_error_as_connection_status_message():
    traced_exc = AirbyteTracedException(
        "an internal message",
        message="Config validation error",
        failure_type=FailureType.config_error,
    )
    airbyte_message = traced_exc.as_connection_status_message()

    assert isinstance(airbyte_message, AirbyteMessage)
    assert airbyte_message.type == MessageType.CONNECTION_STATUS
    assert airbyte_message.connectionStatus.status == Status.FAILED
    assert airbyte_message.connectionStatus.message == "Config validation error"


def test_other_error_as_connection_status_message():
    traced_exc = AirbyteTracedException(
        "an internal message", failure_type=FailureType.system_error
    )
    airbyte_message = traced_exc.as_connection_status_message()

    assert airbyte_message is None


def test_emit_message(capsys):
    traced_exc = AirbyteTracedException(
        internal_message="internal message",
        message="user-friendly message",
        exception=RuntimeError("oh no"),
    )

    expected_message = AirbyteMessage(
        type=MessageType.TRACE,
        trace=AirbyteTraceMessage(
            type=TraceType.ERROR,
            emitted_at=0.0,
            error=AirbyteErrorTraceMessage(
                failure_type=FailureType.system_error,
                message="user-friendly message",
                internal_message="internal message",
                stack_trace="RuntimeError: oh no\n",
            ),
        ),
    )

    traced_exc.emit_message()

    stdout = capsys.readouterr().out
    printed_message = AirbyteMessageSerializer.load(orjson.loads(stdout))
    printed_message.trace.emitted_at = 0.0
    assert printed_message == expected_message


def test_given_both_init_and_as_message_with_stream_descriptor_when_as_airbyte_message_use_init_stream_descriptor() -> (
    None
):
    traced_exc = AirbyteTracedException(stream_descriptor=_A_STREAM_DESCRIPTOR)
    message = traced_exc.as_airbyte_message(stream_descriptor=_ANOTHER_STREAM_DESCRIPTOR)
    assert message.trace.error.stream_descriptor == _A_STREAM_DESCRIPTOR


def test_given_both_init_and_as_sanitized_airbyte_message_with_stream_descriptor_when_as_airbyte_message_use_init_stream_descriptor() -> (
    None
):
    traced_exc = AirbyteTracedException(stream_descriptor=_A_STREAM_DESCRIPTOR)
    message = traced_exc.as_sanitized_airbyte_message(stream_descriptor=_ANOTHER_STREAM_DESCRIPTOR)
    assert message.trace.error.stream_descriptor == _A_STREAM_DESCRIPTOR


def test_given_both_from_exception_and_as_message_with_stream_descriptor_when_as_airbyte_message_use_init_stream_descriptor() -> (
    None
):
    traced_exc = AirbyteTracedException.from_exception(
        _AN_EXCEPTION, stream_descriptor=_A_STREAM_DESCRIPTOR
    )
    message = traced_exc.as_airbyte_message(stream_descriptor=_ANOTHER_STREAM_DESCRIPTOR)
    assert message.trace.error.stream_descriptor == _A_STREAM_DESCRIPTOR


def test_given_both_from_exception_and_as_sanitized_airbyte_message_with_stream_descriptor_when_as_airbyte_message_use_init_stream_descriptor() -> (
    None
):
    traced_exc = AirbyteTracedException.from_exception(
        _AN_EXCEPTION, stream_descriptor=_A_STREAM_DESCRIPTOR
    )
    message = traced_exc.as_sanitized_airbyte_message(stream_descriptor=_ANOTHER_STREAM_DESCRIPTOR)
    assert message.trace.error.stream_descriptor == _A_STREAM_DESCRIPTOR


class TestAirbyteTracedExceptionStr:
    """Tests proving that __str__ returns user-facing message instead of internal_message."""

    def test_str_returns_user_facing_message_when_both_set(self) -> None:
        exc = AirbyteTracedException(
            internal_message="raw API error: 401 Unauthorized",
            message="Authentication credentials are invalid.",
        )
        assert str(exc) == "Authentication credentials are invalid."

    def test_str_falls_back_to_internal_message_when_message_is_none(self) -> None:
        exc = AirbyteTracedException(internal_message="an internal error")
        assert str(exc) == "an internal error"

    def test_str_returns_empty_string_when_both_none(self) -> None:
        exc = AirbyteTracedException()
        assert str(exc) == ""

    def test_str_returns_message_when_internal_message_is_none(self) -> None:
        exc = AirbyteTracedException(message="A user-friendly error occurred.")
        assert str(exc) == "A user-friendly error occurred."

    def test_str_used_in_fstring_returns_user_facing_message(self) -> None:
        exc = AirbyteTracedException(
            internal_message="internal detail",
            message="Connection timed out.",
        )
        assert f"Error: {exc}" == "Error: Connection timed out."

    def test_str_used_in_logging_format_returns_user_facing_message(self) -> None:
        exc = AirbyteTracedException(
            internal_message="socket.timeout: read timed out",
            message="Request timed out.",
        )
        assert "Error: %s" % exc == "Error: Request timed out."

    def test_args_still_contains_internal_message(self) -> None:
        """Verify args[0] is still internal_message for traceback formatting."""
        exc = AirbyteTracedException(
            internal_message="internal detail",
            message="user-facing message",
        )
        assert exc.args[0] == "internal detail"

    def test_str_on_subclass_inherits_behavior(self) -> None:
        """Verify subclasses inherit the __str__ override without needing their own."""

        class CustomTracedException(AirbyteTracedException):
            pass

        exc = CustomTracedException(
            internal_message="raw error",
            message="User-friendly error.",
        )
        assert str(exc) == "User-friendly error."

    def test_str_with_from_exception_factory(self) -> None:
        original = ValueError("original error")
        exc = AirbyteTracedException.from_exception(
            original, message="A validation error occurred."
        )
        assert str(exc) == "A validation error occurred."
        assert exc.internal_message == "original error"

    def test_str_with_from_exception_without_message(self) -> None:
        original = RuntimeError("runtime failure")
        exc = AirbyteTracedException.from_exception(original)
        assert str(exc) == "runtime failure"

    def test_stack_trace_uses_str_representation(self) -> None:
        """Verify traceback one-liner uses __str__ (user-facing message)."""
        exc = AirbyteTracedException(
            internal_message="internal detail for traceback",
            message="User sees this.",
        )
        airbyte_message = exc.as_airbyte_message()
        assert "User sees this." in airbyte_message.trace.error.stack_trace

    def test_str_with_empty_message_does_not_fall_back_to_internal_message(self) -> None:
        """Explicit empty message should be respected and not replaced by internal_message."""
        exc = AirbyteTracedException(
            internal_message="an internal error that should not be shown to the user",
            message="",
        )
        assert str(exc) == ""

    def test_internal_message_preserved_in_trace_error(self) -> None:
        """Verify internal_message is still available in the trace error for debugging."""
        exc = AirbyteTracedException(
            internal_message="raw API error: 401",
            message="Authentication failed.",
        )
        airbyte_message = exc.as_airbyte_message()
        assert airbyte_message.trace.error.internal_message == "raw API error: 401"
        assert airbyte_message.trace.error.message == "Authentication failed."


class TestFromExceptionPreservesFields:
    """Tests proving that from_exception preserves both fields when wrapping an AirbyteTracedException."""

    def test_from_exception_wrapping_traced_preserves_internal_message(self) -> None:
        """When wrapping an AirbyteTracedException, internal_message should be taken directly, not via str()."""
        original = AirbyteTracedException(
            internal_message="raw API error: 401 Unauthorized",
            message="Authentication failed.",
        )
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.internal_message == "raw API error: 401 Unauthorized"

    def test_from_exception_wrapping_traced_preserves_message(self) -> None:
        """When wrapping an AirbyteTracedException without explicit message, the original's message is preserved."""
        original = AirbyteTracedException(
            internal_message="raw API error",
            message="User-friendly error.",
        )
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.message == "User-friendly error."

    def test_from_exception_wrapping_traced_caller_message_overrides(self) -> None:
        """When the caller provides an explicit message, it should override the original's message."""
        original = AirbyteTracedException(
            internal_message="raw API error",
            message="Original user message.",
        )
        wrapped = AirbyteTracedException.from_exception(original, message="Custom wrapper message.")
        assert wrapped.message == "Custom wrapper message."
        assert wrapped.internal_message == "raw API error"

    def test_from_exception_wrapping_traced_with_only_message(self) -> None:
        """When wrapping a traced exception that has message but no internal_message, both fields are preserved."""
        original = AirbyteTracedException(
            message="User error only.",
        )
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.internal_message is None
        assert wrapped.message == "User error only."

    def test_from_exception_wrapping_traced_with_only_internal_message(self) -> None:
        """When wrapping a traced exception that has only internal_message, it is preserved correctly."""
        original = AirbyteTracedException(
            internal_message="internal detail only",
        )
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.internal_message == "internal detail only"
        assert wrapped.message is None

    def test_from_exception_wrapping_traced_with_neither_field(self) -> None:
        """When wrapping a traced exception with no messages, both remain None."""
        original = AirbyteTracedException()
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.internal_message is None
        assert wrapped.message is None

    def test_from_exception_wrapping_regular_exception_unchanged(self) -> None:
        """Wrapping a regular exception should still use str(exc) for internal_message."""
        original = ValueError("some value error")
        wrapped = AirbyteTracedException.from_exception(original)
        assert wrapped.internal_message == "some value error"
        assert wrapped.message is None

    def test_from_exception_wrapping_regular_exception_with_message(self) -> None:
        """Wrapping a regular exception with explicit message kwarg still works."""
        original = RuntimeError("runtime failure")
        wrapped = AirbyteTracedException.from_exception(
            original, message="A runtime error occurred."
        )
        assert wrapped.internal_message == "runtime failure"
        assert wrapped.message == "A runtime error occurred."
