from unittest.mock import MagicMock

import pytest
import requests_cache

from airbyte_cdk.sources.streams.http.http_client import monkey_patched_get_item


def test_assert_requests_cache_version():
    """
    We need to be alerted once the requests_cache version is updated. The reason is that we monkey patch one of the
    method in order to fix a bug until a new version is released.

    For more information about the reasons of this test, see monkey_patched_get_item in http_client.py
    """
    assert requests_cache.__version__ == "1.2.1"


def test_monkey_patched_get_item_raises_key_error_on_missing_row():
    """Verify that a missing cache key raises KeyError (standard cache miss)."""
    mock_self = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_self.connection().__enter__().execute.return_value = mock_cursor

    with pytest.raises(KeyError):
        monkey_patched_get_item(mock_self, "missing_key")


def test_monkey_patched_get_item_returns_deserialized_value():
    """Verify that a valid cache entry is deserialized and returned."""
    mock_self = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("serialized_data",)
    mock_self.connection().__enter__().execute.return_value = mock_cursor
    mock_self.deserialize.return_value = "deserialized_value"

    result = monkey_patched_get_item(mock_self, "valid_key")

    assert result == "deserialized_value"
    mock_self.deserialize.assert_called_once_with("valid_key", "serialized_data")


def test_monkey_patched_get_item_converts_eoferror_to_key_error():
    """
    Verify that EOFError during deserialization of corrupted cache data
    is caught and converted to KeyError, treating it as a cache miss.

    This handles the case where fast_save=True + synchronous=OFF leads
    to truncated pickle data in the SQLite cache.
    """
    mock_self = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("corrupted_data",)
    mock_self.connection().__enter__().execute.return_value = mock_cursor
    mock_self.deserialize.side_effect = EOFError("Ran out of input")

    with pytest.raises(KeyError):
        monkey_patched_get_item(mock_self, "corrupted_key")
