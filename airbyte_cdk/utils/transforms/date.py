from __future__ import annotations
from typing import Any, Dict, Optional

def try_parse_date(value: Any):
    # accept datetime/pandas/pendulum-like objects; else None
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value
    return None

def extract_date_parts(dt) -> Dict[str, Optional[int]]:
    try:
        return {"year": dt.year, "month": dt.month, "day": dt.day, "dow": int(dt.weekday())}
    except Exception:
        return {"year": None, "month": None, "day": None, "dow": None}

def floor_to_month(dt):
    try:
        return dt.replace(day=1)
    except Exception:
        return None

def ceil_to_month(dt):
    try:
        if dt.month == 12:
            return dt.replace(year=dt.year + 1, month=1, day=1)
        return dt.replace(month=dt.month + 1, day=1)
    except Exception:
        return None
