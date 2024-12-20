#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.declarative.transformations.keys_replace_transformation import (
    KeysReplaceTransformation,
)

_ANY_VALUE = -1


def test_transform():
    record = {"date time": _ANY_VALUE, "customer id": _ANY_VALUE}
    KeysReplaceTransformation(old=" ", new="_").transform(record)
    assert record == {"date_time": _ANY_VALUE, "customer_id": _ANY_VALUE}
