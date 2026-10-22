#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

from typing import IO, Any, Optional, Union, cast

import yaml


def _get_safe_loader() -> Union[type[yaml.SafeLoader], type[yaml.CSafeLoader]]:
    """Return libyaml's CSafeLoader when PyYAML was built with it, else the pure-Python SafeLoader."""
    # The annotations stay unevaluated: yaml.CSafeLoader does not exist without libyaml.
    return cast(
        "Union[type[yaml.SafeLoader], type[yaml.CSafeLoader]]",
        getattr(yaml, "CSafeLoader", yaml.SafeLoader),
    )


def _tell_if_seekable(stream: Union[IO[str], IO[bytes]]) -> Optional[int]:
    """Return the stream's current position if it can be rewound, else None."""
    try:
        if stream.seekable() is not True:
            return None
        position = stream.tell()
    except (AttributeError, OSError, ValueError):
        return None
    return position if isinstance(position, int) else None


def safe_load_yaml(stream: Union[str, bytes, IO[str], IO[bytes]]) -> Any:
    """Parse trusted YAML like yaml.safe_load, using libyaml's CSafeLoader when available.

    Results match yaml.safe_load for documents both loaders accept. libyaml accepts
    some input SafeLoader rejects (for example a tab after `key:`), ignores constructors
    and resolvers registered on yaml.SafeLoader, and can crash the process on very deeply
    nested input, so do not use this for untrusted input. When libyaml raises a
    YAMLError, the input is parsed again with SafeLoader so callers see SafeLoader's error.
    """
    loader = _get_safe_loader()
    exact_text = type(stream) in (str, bytes)
    rewind_to = None
    if not exact_text and not isinstance(stream, (str, bytes)):
        rewind_to = _tell_if_seekable(stream)
    # str/bytes subclasses, non-seekable streams and mocks keep today's behaviour.
    if loader is yaml.SafeLoader or (not exact_text and rewind_to is None):
        return yaml.load(stream, Loader=yaml.SafeLoader)
    try:
        return yaml.load(stream, Loader=loader)
    except yaml.YAMLError:
        pass  # Re-parse outside the handler so libyaml's error is not chained.
    if rewind_to is not None:
        cast(Union[IO[str], IO[bytes]], stream).seek(rewind_to)
    return yaml.load(stream, Loader=yaml.SafeLoader)
