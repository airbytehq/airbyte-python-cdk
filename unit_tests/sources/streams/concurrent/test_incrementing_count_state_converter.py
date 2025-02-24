from airbyte_cdk.sources.streams.concurrent.cursor import CursorField
from airbyte_cdk.sources.streams.concurrent.state_converters.incrementing_count_stream_state_converter import (
    IncrementingCountStreamStateConverter,
)


def test_convert_from_sequential_state():
    converter = IncrementingCountStreamStateConverter(
        is_sequential_state=True,
    )

    _, conversion = converter.convert_from_sequential_state(CursorField("id"), {"id": 12345}, 0)

    assert conversion["state_type"] == "integer"
    assert conversion["legacy"] == {"id": 12345}
    assert len(conversion["slices"]) == 1
    assert conversion["slices"][0] == {"end": 12345, "most_recent_cursor_value": 12345, "start": 0}


def test_convert_to_sequential_state():
    converter = IncrementingCountStreamStateConverter(
        is_sequential_state=True,
    )
    concurrent_state = {
        "legacy": {"id": 12345},
        "slices": [{"end": 12345, "most_recent_cursor_value": 12345, "start": 0}],
        "state_type": "integer",
    }
    assert converter.convert_to_state_message(CursorField("id"), concurrent_state) == {"id": 12345}
