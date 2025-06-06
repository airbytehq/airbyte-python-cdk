# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""This module is deprecated and exists only for legacy compatibility.

Instead of importing from this module, callers should import from
`airbyte_cdk.models.airbyte_protocol` directly.

The dedicated SerDes classes are _also_ deprecated. Instead, use these methods:
- `from_dict()`
- `from_json()`
- `to_dict()`
- `to_json()`
"""

from airbyte_cdk.models.airbyte_protocol import *  # type: ignore[attr-defined]
