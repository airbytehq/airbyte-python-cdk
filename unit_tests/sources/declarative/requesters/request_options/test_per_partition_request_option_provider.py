from unittest import TestCase
from unittest.mock import Mock

from airbyte_cdk.sources.declarative.partition_routers import PartitionRouter
from airbyte_cdk.sources.declarative.requesters.request_options import RequestOptionsProvider
from airbyte_cdk.sources.declarative.requesters.request_options.per_partition_request_option_provider import (
    PerPartitionRequestOptionsProvider,
)
from airbyte_cdk.sources.declarative.types import StreamSlice

_STREAM_STATE = {"state_key": "state_value"}
_STREAM_SLICE = StreamSlice(partition={"slice_key": "slice_value"}, cursor_slice={})
_NEXT_PAGE_TOKEN = {"page_token_key": "page_token_value"}


class TestPerPartitionRequestOptionsProvider(TestCase):
    def setUp(self):
        self._partition_router = Mock(spec=PartitionRouter)
        self._cursor_provider = Mock(spec=RequestOptionsProvider)
        self._option_provider = PerPartitionRequestOptionsProvider(
            self._partition_router, self._cursor_provider
        )

    def test_given_partition_router_value_when_get_request_params_then_return_partition_router_params(
        self,
    ) -> None:
        self._partition_router.get_request_params.return_value = {"key": "value"}
        self._cursor_provider.get_request_params.return_value = dict()

        result = self._option_provider.get_request_params(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_cursor_provider_value_when_get_request_params_then_return_partition_router_params(
        self,
    ) -> None:
        self._partition_router.get_request_params.return_value = dict()
        self._cursor_provider.get_request_params.return_value = {"key": "value"}

        result = self._option_provider.get_request_params(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_both_provide_value_when_get_request_params_then_overwrite_from_cursor(
        self,
    ) -> None:
        self._partition_router.get_request_params.return_value = {
            "key_duplicate": "value_partition",
            "key_partition": "value_partition",
        }
        self._cursor_provider.get_request_params.return_value = {
            "key_duplicate": "value_cursor",
            "key_cursor": "value_cursor",
        }

        result = self._option_provider.get_request_params(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {
            "key_duplicate": "value_cursor",
            "key_partition": "value_partition",
            "key_cursor": "value_cursor",
        }

    def test_given_partition_router_value_when_get_request_headers_then_return_partition_router_headers(
        self,
    ) -> None:
        self._partition_router.get_request_headers.return_value = {"key": "value"}
        self._cursor_provider.get_request_headers.return_value = dict()

        result = self._option_provider.get_request_headers(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_cursor_provider_value_when_get_request_headers_then_return_cursor_provider_headers(
        self,
    ) -> None:
        self._partition_router.get_request_headers.return_value = dict()
        self._cursor_provider.get_request_headers.return_value = {"key": "value"}

        result = self._option_provider.get_request_headers(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_both_provide_value_when_get_request_headers_then_overwrite_from_cursor(
        self,
    ) -> None:
        self._partition_router.get_request_headers.return_value = {
            "key_duplicate": "value_partition",
            "key_partition": "value_partition",
        }
        self._cursor_provider.get_request_headers.return_value = {
            "key_duplicate": "value_cursor",
            "key_cursor": "value_cursor",
        }

        result = self._option_provider.get_request_headers(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {
            "key_duplicate": "value_cursor",
            "key_partition": "value_partition",
            "key_cursor": "value_cursor",
        }

    def test_given_partition_router_value_when_get_request_body_data_then_return_partition_router_body_data(
        self,
    ) -> None:
        self._partition_router.get_request_body_data.return_value = {"key": "value"}
        self._cursor_provider.get_request_body_data.return_value = dict()

        result = self._option_provider.get_request_body_data(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_cursor_provider_value_when_get_request_body_data_then_return_cursor_provider_body_data(
        self,
    ) -> None:
        self._partition_router.get_request_body_data.return_value = dict()
        self._cursor_provider.get_request_body_data.return_value = {"key": "value"}

        result = self._option_provider.get_request_body_data(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_both_provide_value_when_get_request_body_data_then_overwrite_from_cursor(
        self,
    ) -> None:
        self._partition_router.get_request_body_data.return_value = {
            "key_duplicate": "value_partition",
            "key_partition": "value_partition",
        }
        self._cursor_provider.get_request_body_data.return_value = {
            "key_duplicate": "value_cursor",
            "key_cursor": "value_cursor",
        }

        result = self._option_provider.get_request_body_data(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {
            "key_duplicate": "value_cursor",
            "key_partition": "value_partition",
            "key_cursor": "value_cursor",
        }

    def test_given_partition_router_value_when_get_request_body_json_then_return_partition_router_body_json(
        self,
    ) -> None:
        self._partition_router.get_request_body_json.return_value = {"key": "value"}
        self._cursor_provider.get_request_body_json.return_value = dict()

        result = self._option_provider.get_request_body_json(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_cursor_provider_value_when_get_request_body_json_then_return_cursor_provider_body_json(
        self,
    ) -> None:
        self._partition_router.get_request_body_json.return_value = dict()
        self._cursor_provider.get_request_body_json.return_value = {"key": "value"}

        result = self._option_provider.get_request_body_json(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {"key": "value"}

    def test_given_both_provide_value_when_get_request_body_json_then_overwrite_from_cursor(
        self,
    ) -> None:
        self._partition_router.get_request_body_json.return_value = {
            "key_duplicate": "value_partition",
            "key_partition": "value_partition",
        }
        self._cursor_provider.get_request_body_json.return_value = {
            "key_duplicate": "value_cursor",
            "key_cursor": "value_cursor",
        }

        result = self._option_provider.get_request_body_json(
            stream_state=_STREAM_STATE,
            stream_slice=_STREAM_SLICE,
            next_page_token=_NEXT_PAGE_TOKEN,
        )

        assert result == {
            "key_duplicate": "value_cursor",
            "key_partition": "value_partition",
            "key_cursor": "value_cursor",
        }
