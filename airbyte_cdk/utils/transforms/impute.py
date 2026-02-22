from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from statistics import mean, median
from collections import Counter
import math

Number = Union[int, float]

@dataclass
class ImputationReport:
    field: str
    strategy: str
    value_used: Any
    notes: str = ""

def _numeric_skewness(values: List[Number]) -> float:
    n = len(values)
    if n < 3: return 0.0
    mu = mean(values)
    var = sum((x - mu) ** 2 for x in values) / (n - 1)
    if var == 0: return 0.0
    sd = math.sqrt(var)
    m3 = sum((x - mu) ** 3 for x in values) / n
    g1 = m3 / (sd ** 3)
    return float(((n * (n - 1)) ** 0.5 / (n - 2)) * g1)

def choose_imputation_strategy(
    series: Sequence[Any],
    numeric: bool | None = None,
    skew_threshold: float = 0.75,
    unique_ratio_threshold: float = 0.05,
) -> str:
    data = [x for x in series if x is not None]
    if not data:
        return "mode"
    if numeric is None:
        numeric = all(isinstance(x, (int, float)) for x in data)
    if not numeric:
        return "mode"
    uniq = len(set(data))
    if (uniq / max(len(data), 1)) <= unique_ratio_threshold:
        return "mode"
    skew = abs(_numeric_skewness([float(x) for x in data]))
    return "median" if skew > skew_threshold else "mean"

def compute_imputation_value(series: Sequence[Any], strategy: str) -> Any:
    clean = [x for x in series if x is not None]
    if not clean:
        return None
    if strategy == "mean":
        nums = [float(x) for x in clean if isinstance(x, (int, float))]
        return mean(nums) if nums else None
    if strategy == "median":
        nums = [float(x) for x in clean if isinstance(x, (int, float))]
        return median(nums) if nums else None
    if strategy == "mode":
        counts = Counter(clean)
        maxc = max(counts.values())
        # deterministic tie-break
        return sorted([k for k, v in counts.items() if v == maxc], key=lambda x: repr(x))[0]
    raise ValueError(f"Unknown strategy: {strategy}")

def fill_nulls_column(
    series: Sequence[Any],
    explicit_strategy: Optional[str] = None,
    numeric: Optional[bool] = None,
    **choose_kwargs
) -> Tuple[List[Any], ImputationReport]:
    strategy = explicit_strategy or choose_imputation_strategy(series, numeric=numeric, **choose_kwargs)
    fill_value = compute_imputation_value(series, strategy)
    return [fill_value if x is None else x for x in series], ImputationReport("<series>", strategy, fill_value)

def fill_nulls_record(
    record: Dict[str, Any],
    columns: Sequence[str],
    samples: Mapping[str, Sequence[Any]],
    strategies: Optional[Mapping[str, str]] = None,
    choose_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[ImputationReport]]:
    choose_kwargs = choose_kwargs or {}
    out = dict(record)
    reports: List[ImputationReport] = []
    for col in columns:
        series = samples.get(col, [])
        sflag = strategies.get(col) if strategies else None
        # infer numeric from samples if not set
        numeric = all(isinstance(x, (int, float)) for x in series if x is not None) if series else None
        # Use sample data for imputation, but only fill the record's value
        strategy = sflag or choose_imputation_strategy(series, numeric=numeric, **choose_kwargs)
        fill_value = compute_imputation_value(series, strategy=strategy)
        out[col] = record.get(col) if record.get(col) is not None else fill_value
        reports.append(ImputationReport(col, strategy, fill_value))
    return out, reports
