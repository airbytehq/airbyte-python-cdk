#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import IO, Any, Union

import yaml


def get_safe_loader() -> Any:
    """Return libyaml's CSafeLoader when PyYAML was built with it, else the pure-Python SafeLoader."""
    return getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def safe_load_yaml(stream: Union[str, bytes, IO[str], IO[bytes]]) -> Any:
    """Equivalent to yaml.safe_load, but uses the C loader when available."""
    return yaml.load(stream, Loader=get_safe_loader())
