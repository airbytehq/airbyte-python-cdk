#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import pytest

from airbyte_cdk.sources.declarative.transformations.keys_replace_transformation import (
    KeysReplaceTransformation,
)

_ANY_VALUE = -1


@pytest.mark.parametrize(
    [
        "input_record",
        "config",
        "stream_state",
        "stream_slice",
        "keys_replace_config",
        "expected_record",
    ],
    [
        pytest.param(
            {"date time": _ANY_VALUE, "customer id": _ANY_VALUE},
            {},
            {},
            {},
            {"old": " ", "new": "_"},
            {"date_time": _ANY_VALUE, "customer_id": _ANY_VALUE},
            id="simple keys replace config",
        ),
        pytest.param(
            {
                "customer_id": 111111,
                "customer_name": "MainCustomer",
                "field_1_111111": _ANY_VALUE,
                "field_2_111111": _ANY_VALUE,
            },
            {},
            {},
            {},
            {"old": '{{ record["customer_id"] }}', "new": '{{ record["customer_name"] }}'},
            {
                "customer_id": 111111,
                "customer_name": "MainCustomer",
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from record",
        ),
        pytest.param(
            {"customer_id": 111111, "field_1_111111": _ANY_VALUE, "field_2_111111": _ANY_VALUE},
            {},
            {},
            {"customer_name": "MainCustomer"},
            {"old": '{{ record["customer_id"] }}', "new": '{{ stream_slice["customer_name"] }}'},
            {
                "customer_id": 111111,
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from slice",
        ),
        pytest.param(
            {"customer_id": 111111, "field_1_111111": _ANY_VALUE, "field_2_111111": _ANY_VALUE},
            {"customer_name": "MainCustomer"},
            {},
            {},
            {"old": '{{ record["customer_id"] }}', "new": '{{ config["customer_name"] }}'},
            {
                "customer_id": 111111,
                "field_1_MainCustomer": _ANY_VALUE,
                "field_2_MainCustomer": _ANY_VALUE,
            },
            id="keys replace config uses values from config",
        ),
        pytest.param(
            {
                "date time": _ANY_VALUE,
                "user id": _ANY_VALUE,
                "customer": {
                    "customer name": _ANY_VALUE,
                    "customer id": _ANY_VALUE,
                    "contact info": {"email": _ANY_VALUE, "phone number": _ANY_VALUE},
                },
            },
            {},
            {},
            {},
            {"old": " ", "new": "_"},
            {
                "customer": {
                    "contact_info": {"email": _ANY_VALUE, "phone_number": _ANY_VALUE},
                    "customer_id": _ANY_VALUE,
                    "customer_name": _ANY_VALUE,
                },
                "date_time": _ANY_VALUE,
                "user_id": _ANY_VALUE,
            },
            id="simple keys replace config with nested fields in record",
        ),
    ],
)
def test_transform(
    input_record, config, stream_state, stream_slice, keys_replace_config, expected_record
):
    KeysReplaceTransformation(
        old=keys_replace_config["old"], new=keys_replace_config["new"], parameters={}
    ).transform(
        record=input_record, config=config, stream_state=stream_state, stream_slice=stream_slice
    )
    assert input_record == expected_record
