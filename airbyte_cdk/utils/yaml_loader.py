#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import IO, Any, Optional, Union, cast

import yaml


def get_safe_loader() -> Any:
    """Return libyaml's CSafeLoader when PyYAML was built with it, else the pure-Python SafeLoader."""
    return getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def _tell_if_seekable(stream: Any) -> Optional[int]:
    """Return the stream's current position if it can be rewound, else None."""
    if isinstance(stream, (str, bytes)):
        return None
    seekable = getattr(stream, "seekable", None)
    if not callable(seekable) or not seekable():
        return None
    try:
        return cast(int, stream.tell())
    except (OSError, ValueError):
        return None


def safe_load_yaml(stream: Union[str, bytes, IO[str], IO[bytes]]) -> Any:
    """Equivalent to yaml.safe_load, but uses the C loader when available.

    If the C loader raises a yaml.YAMLError, the input is re-parsed with the
    pure-Python SafeLoader and that error is raised instead. The C loader's
    messages lack the source-line snippet and differ in wording, so the re-parse
    preserves the error messages users saw before the C loader was introduced.
    """
    loader = get_safe_loader()
    if loader is yaml.SafeLoader:
        return yaml.load(stream, Loader=yaml.SafeLoader)
    rewind_to = _tell_if_seekable(stream)
    try:
        return yaml.load(stream, Loader=loader)
    except yaml.YAMLError:
        if isinstance(stream, (str, bytes)):
            return yaml.load(stream, Loader=yaml.SafeLoader)
        if rewind_to is None:
            raise
        stream.seek(rewind_to)
        return yaml.load(stream, Loader=yaml.SafeLoader)
