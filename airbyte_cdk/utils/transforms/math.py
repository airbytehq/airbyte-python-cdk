from __future__ import annotations
from typing import Sequence, Tuple, Union
import math

Number = Union[int, float]

def minmax_scale(x: Number, data_min: Number, data_max: Number, out_range: Tuple[Number, Number]=(0.0, 1.0)) -> float:
    a, b = out_range
    if data_max == data_min:
        return float(a + (b - a) / 2.0)
    return ((float(x) - data_min) / (data_max - data_min)) * (b - a) + a

def zscore(x: Number, mu: float, sigma: float) -> float:
    return 0.0 if sigma == 0 else (float(x) - mu) / sigma

def clip(x: Number, low: Number, high: Number) -> Number:
    return max(low, min(high, x))

def winsorize(x: Number, low_value: Number, high_value: Number) -> Number:
    return clip(x, low_value, high_value)

def log1p_safe(x: Number) -> float:
    if x < -1:
        return float(x)
    try:
        return math.log1p(float(x))
    except Exception:
        return float(x)

def bucketize(x: Number, edges: Sequence[Number]) -> int:
    for i, e in enumerate(edges):
        if x <= e:
            return i
    return len(edges)

def robust_percentile_scale(
    x: Number,
    p_low_value: Number,
    p_high_value: Number,
    out_range: Tuple[Number, Number]=(0.0, 1.0),
    clip_outliers: bool=True
) -> float:
    a, b = out_range
    lo, hi = float(p_low_value), float(p_high_value)
    if clip_outliers:
        x = clip(float(x), lo, hi)
    width = hi - lo
    if width == 0:
        return float(a + (b - a) / 2.0)
    return ((float(x) - lo) / width) * (b - a) + a
