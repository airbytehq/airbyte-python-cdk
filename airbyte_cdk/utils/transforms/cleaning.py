from __future__ import annotations
from typing import Any, Mapping, Optional, Union
import re, unicodedata

Number = Union[int, float]

def to_lower(s: Optional[str]) -> Optional[str]:
    return None if s is None else s.lower()

def strip_whitespace(s: Optional[str]) -> Optional[str]:
    return None if s is None else s.strip()

def squash_whitespace(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()

def normalize_unicode(s: Optional[str], form: str="NFKC") -> Optional[str]:
    return None if s is None else unicodedata.normalize(form, s)

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
def remove_punctuation(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return _PUNCT_RE.sub("", s)

def map_values(value: Any, mapping: Mapping[Any, Any], default: Any=None) -> Any:
    return mapping.get(value, default)

def cast_numeric(value: Any, on_error: str="ignore", default: Optional[Number]=None) -> Optional[Number]:
    try:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise ValueError("empty")
        f = float(value)
        i = int(f)
        return i if i == f else f
    except Exception:
        if on_error == "default":
            return default
        if on_error == "none":
            return None
        if on_error == "raise":
            raise
        return value
