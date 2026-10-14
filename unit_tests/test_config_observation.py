#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import sys
import time
from io import StringIO
from threading import Barrier, Thread

import pytest

from airbyte_cdk.config_observation import (
    ConfigObserver,
    ObservedDict,
    create_connector_config_control_message,
    emit_configuration_as_airbyte_control_message,
    observe_connector_config,
)
from airbyte_cdk.models import AirbyteControlConnectorConfigMessage, OrchestratorType, Type
from airbyte_cdk.utils.print_buffer import PrintBuffer


class TestObservedDict:
    def test_update_called_on_set_item(self, mocker):
        mock_observer = mocker.Mock()
        my_observed_dict = ObservedDict(
            {
                "key": "value",
                "nested_dict": {"key": "value"},
                "list_of_dict": [{"key": "value"}, {"key": "value"}],
            },
            mock_observer,
        )
        assert mock_observer.update.call_count == 0

        my_observed_dict["nested_dict"]["key"] = "new_value"
        assert mock_observer.update.call_count == 1

        # Setting the same value again should call observer's update
        my_observed_dict["key"] = "new_value"
        assert mock_observer.update.call_count == 2

        my_observed_dict["nested_dict"]["new_key"] = "value"
        assert mock_observer.update.call_count == 3

        my_observed_dict["list_of_dict"][0]["key"] = "new_value"
        assert mock_observer.update.call_count == 4

        my_observed_dict["list_of_dict"][0]["new_key"] = "new_value"
        assert mock_observer.update.call_count == 5

        my_observed_dict["new_list_of_dicts"] = [{"foo": "bar"}]
        assert mock_observer.update.call_count == 6

        my_observed_dict["new_list_of_dicts"][0]["new_key"] = "new_value"
        assert mock_observer.update.call_count == 7


class TestConfigObserver:
    def test_update(self, capsys):
        config_observer = ConfigObserver()
        config_observer.set_config(ObservedDict({"key": "value"}, config_observer))
        before_time = time.time() * 1000
        config_observer.update()
        after_time = time.time() * 1000
        captured = capsys.readouterr()
        airbyte_message = json.loads(captured.out)
        assert airbyte_message["type"] == "CONTROL"
        assert "control" in airbyte_message
        raw_control_message = airbyte_message["control"]
        assert raw_control_message["type"] == "CONNECTOR_CONFIG"
        assert raw_control_message["connectorConfig"] == {"config": dict(config_observer.config)}
        assert before_time < raw_control_message["emitted_at"] < after_time


def test_observe_connector_config(capsys):
    non_observed_config = {"foo": "bar"}
    observed_config = observe_connector_config(non_observed_config)
    observer = observed_config.observer
    assert isinstance(observed_config, ObservedDict)
    assert isinstance(observer, ConfigObserver)
    assert observed_config.observer.config == observed_config
    observed_config["foo"] = "foo"
    captured = capsys.readouterr()
    airbyte_message = json.loads(captured.out)
    assert airbyte_message["control"]["connectorConfig"] == {"config": {"foo": "foo"}}


def test_observe_already_observed_config():
    observed_config = observe_connector_config({"foo": "bar"})
    with pytest.raises(ValueError):
        observe_connector_config(observed_config)


def test_create_connector_config_control_message():
    A_CONFIG = {"config key": "config value"}

    message = create_connector_config_control_message(A_CONFIG)

    assert message.type == Type.CONTROL
    assert message.control.type == OrchestratorType.CONNECTOR_CONFIG
    assert message.control.connectorConfig == AirbyteControlConnectorConfigMessage(config=A_CONFIG)
    assert message.control.emitted_at is not None


def test_emit_configuration_as_airbyte_control_message_is_line_atomic(monkeypatch):
    writes = []

    class RecordingStream:
        def write(self, message):
            writes.append(message)

    monkeypatch.setattr(sys, "stdout", RecordingStream())

    emit_configuration_as_airbyte_control_message({"foo": "bar"})

    assert len(writes) == 1
    assert writes[0].endswith("\n")
    assert writes[0].count("\n") == 1
    assert json.loads(writes[0])["type"] == "CONTROL"


def test_emit_configuration_as_airbyte_control_message_concurrent_output_is_well_framed(
    monkeypatch,
):
    captured_output = []
    print_buffer = PrintBuffer(flush_interval=float("inf"))

    def capture_flush():
        captured_output.append(print_buffer.buffer.getvalue())
        print_buffer.buffer = StringIO()

    monkeypatch.setattr(print_buffer, "flush", capture_flush)
    monkeypatch.setattr(sys, "stdout", print_buffer)

    worker_count = 4
    iterations = 100
    start_barrier = Barrier(worker_count + 1)

    def print_records():
        start_barrier.wait()
        for index in range(worker_count * iterations):
            record = {"type": "RECORD", "record": {"data": {"index": index}}}
            print(f"{json.dumps(record)}\n", end="")

    def emit_control_messages(worker_id):
        start_barrier.wait()
        for index in range(iterations):
            emit_configuration_as_airbyte_control_message({"worker_id": worker_id, "index": index})

    threads = [
        Thread(target=print_records),
        *[
            Thread(target=emit_control_messages, args=(worker_id,))
            for worker_id in range(worker_count)
        ],
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print_buffer.flush()

    output = "".join(captured_output)
    lines = output.split("\n")
    assert lines[-1] == ""
    lines = lines[:-1]
    assert lines
    assert all(line for line in lines)
    assert all(json.loads(line) for line in lines)
