# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from airbyte_cdk.models import AirbyteMessageSerializer, AirbyteStateMessageSerializer


def test_stream_state_message_preserves_additional_properties():
    message = {
        "type": "STATE",
        "state": {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "users", "namespace": "public"},
                "stream_state": {"cursor": "value"},
            },
            "id": 42,
        },
    }

    loaded = AirbyteMessageSerializer.load(message)

    assert AirbyteMessageSerializer.dump(loaded) == message


def test_global_state_message_preserves_additional_properties():
    message = {
        "type": "STATE",
        "state": {
            "type": "GLOBAL",
            "global": {
                "stream_states": [
                    {
                        "stream_descriptor": {"name": "users", "namespace": "public"},
                        "stream_state": {"cursor": "value"},
                    }
                ],
                "shared_state": {"shared": "value"},
            },
            "id": 42,
        },
    }

    loaded = AirbyteMessageSerializer.load(message)

    assert AirbyteMessageSerializer.dump(loaded) == message


def test_direct_state_message_serializer_preserves_additional_properties():
    message = {
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {"name": "users"},
            "stream_state": {"cursor": "value"},
        },
        "id": 42,
    }

    loaded = AirbyteStateMessageSerializer.load(message)

    assert AirbyteStateMessageSerializer.dump(loaded) == message


def test_state_message_without_additional_properties_does_not_leak_attribute():
    message = {
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {"name": "users"},
            "stream_state": {"cursor": "value"},
        },
    }

    loaded = AirbyteStateMessageSerializer.load(message)

    assert AirbyteStateMessageSerializer.dump(loaded) == message
    assert "additional_properties" not in AirbyteStateMessageSerializer.dump(loaded)
