# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from unittest.mock import Mock

from airbyte_cdk.sources.declarative.requesters.query_properties import PropertiesFromEndpoint
from airbyte_cdk.sources.declarative.retrievers import SimpleRetriever
from airbyte_cdk.sources.types import Record, StreamSlice

CONFIG = {}


def test_get_properties_from_endpoint():
    expected_properties = [
        "gentarou",
        "light",
        "aoi",
        "clover",
        "junpei",
        "akane",
        "unknown",
        "hazuki",
        "teruaki",
    ]

    retriever = Mock(spec=SimpleRetriever)
    retriever.read_records.return_value = iter(
        [
            Record(stream_name="players", data={"id": "ace", "name": "gentarou"}),
            Record(stream_name="players", data={"id": "snake", "name": "light"}),
            Record(stream_name="players", data={"id": "santa", "name": "aoi"}),
            Record(stream_name="players", data={"id": "clover", "name": "clover"}),
            Record(stream_name="players", data={"id": "junpei", "name": "junpei"}),
            Record(stream_name="players", data={"id": "june", "name": "akane"}),
            Record(stream_name="players", data={"id": "seven", "name": "unknown"}),
            Record(stream_name="players", data={"id": "lotus", "name": "hazuki"}),
            Record(stream_name="players", data={"id": "nine", "name": "teruaki"}),
        ]
    )

    properties_from_endpoint = PropertiesFromEndpoint(
        retriever=retriever,
        property_field_path=["name"],
        config=CONFIG,
        parameters={},
    )

    properties = properties_from_endpoint.get_properties_from_endpoint()

    assert len(properties) == 9
    assert properties == expected_properties


def test_get_properties_from_endpoint_with_multiple_field_paths():
    expected_properties = [
        "gentarou",
        "light",
        "aoi",
        "clover",
        "junpei",
        "akane",
        "unknown",
        "hazuki",
        "teruaki",
    ]

    retriever = Mock(spec=SimpleRetriever)
    retriever.read_records.return_value = iter(
        [
            Record(stream_name="players", data={"id": "ace", "names": {"first_name": "gentarou"}}),
            Record(stream_name="players", data={"id": "snake", "names": {"first_name": "light"}}),
            Record(stream_name="players", data={"id": "santa", "names": {"first_name": "aoi"}}),
            Record(stream_name="players", data={"id": "clover", "names": {"first_name": "clover"}}),
            Record(stream_name="players", data={"id": "junpei", "names": {"first_name": "junpei"}}),
            Record(stream_name="players", data={"id": "june", "names": {"first_name": "akane"}}),
            Record(stream_name="players", data={"id": "seven", "names": {"first_name": "unknown"}}),
            Record(stream_name="players", data={"id": "lotus", "names": {"first_name": "hazuki"}}),
            Record(stream_name="players", data={"id": "nine", "names": {"first_name": "teruaki"}}),
        ]
    )

    properties_from_endpoint = PropertiesFromEndpoint(
        retriever=retriever,
        property_field_path=["names", "first_name"],
        config=CONFIG,
        parameters={},
    )

    properties = properties_from_endpoint.get_properties_from_endpoint()

    assert len(properties) == 9
    assert properties == expected_properties


def test_get_properties_from_endpoint_with_interpolation():
    config = {"top_level_field": "names"}
    expected_properties = [
        "gentarou",
        "light",
        "aoi",
        "clover",
        "junpei",
        "akane",
        "unknown",
        "hazuki",
        "teruaki",
    ]

    retriever = Mock(spec=SimpleRetriever)
    retriever.read_records.return_value = iter(
        [
            Record(stream_name="players", data={"id": "ace", "names": {"first_name": "gentarou"}}),
            Record(stream_name="players", data={"id": "snake", "names": {"first_name": "light"}}),
            Record(stream_name="players", data={"id": "santa", "names": {"first_name": "aoi"}}),
            Record(stream_name="players", data={"id": "clover", "names": {"first_name": "clover"}}),
            Record(stream_name="players", data={"id": "junpei", "names": {"first_name": "junpei"}}),
            Record(stream_name="players", data={"id": "june", "names": {"first_name": "akane"}}),
            Record(stream_name="players", data={"id": "seven", "names": {"first_name": "unknown"}}),
            Record(stream_name="players", data={"id": "lotus", "names": {"first_name": "hazuki"}}),
            Record(stream_name="players", data={"id": "nine", "names": {"first_name": "teruaki"}}),
        ]
    )

    properties_from_endpoint = PropertiesFromEndpoint(
        retriever=retriever,
        property_field_path=["{{ config['top_level_field'] }}", "first_name"],
        config=config,
        parameters={},
    )

    properties = properties_from_endpoint.get_properties_from_endpoint()

    assert len(properties) == 9
    assert properties == expected_properties


def test_given_multiple_calls_when_get_properties_from_endpoint_then_only_call_retriever_once():
    retriever = Mock(spec=SimpleRetriever)
    retriever.read_records.return_value = iter(
        [
            Record(stream_name="players", data={"id": "ace", "value": 1}),
        ]
    )

    properties_from_endpoint = PropertiesFromEndpoint(
        retriever=retriever,
        property_field_path=["value"],
        config={},
        parameters={},
    )

    properties_from_endpoint.get_properties_from_endpoint()
    properties_from_endpoint.get_properties_from_endpoint()
    properties_from_endpoint.get_properties_from_endpoint()

    assert retriever.read_records.call_count == 1


def test_given_value_is_int_when_get_properties_from_endpoint_then_return_str():
    expected_properties = ["1"]

    retriever = Mock(spec=SimpleRetriever)
    retriever.read_records.return_value = iter(
        [
            Record(stream_name="players", data={"id": "ace", "value": 1}),
        ]
    )

    properties_from_endpoint = PropertiesFromEndpoint(
        retriever=retriever,
        property_field_path=["value"],
        config={},
        parameters={},
    )

    properties = properties_from_endpoint.get_properties_from_endpoint()

    assert properties == expected_properties
