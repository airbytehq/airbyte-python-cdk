"""Unit tests for imputation transforms."""
import pytest
from airbyte_cdk.utils.transforms.impute import (
    _numeric_skewness,
    choose_imputation_strategy,
    compute_imputation_value,
    fill_nulls_column,
    fill_nulls_record,
    ImputationReport,
)

def test_numeric_skewness():
    """Test skewness calculation function."""
    # Test normal cases
    assert _numeric_skewness([1, 2, 3]) == pytest.approx(0.0, abs=1e-10)  # Symmetric data
    assert _numeric_skewness([1, 1, 2]) > 0  # Positive skew
    assert _numeric_skewness([1, 2, 2]) < 0  # Negative skew
    
    # Test edge cases
    assert _numeric_skewness([1, 1]) == 0.0  # Less than 3 values
    assert _numeric_skewness([1, 1, 1]) == 0.0  # No variance
    
    # Test with floating point values
    assert _numeric_skewness([1.0, 2.0, 3.0]) == pytest.approx(0.0, abs=1e-10)

def test_choose_imputation_strategy():
    """Test imputation strategy selection function."""
    # Test numeric data
    assert choose_imputation_strategy([1, 2, 3]) == "mean"  # Low skew
    assert choose_imputation_strategy([1, 1, 10]) == "median"  # High skew
    
    # Test categorical data
    assert choose_imputation_strategy(["a", "b", "c"], numeric=False) == "mode"
    assert choose_imputation_strategy(["a", "a", "b"]) == "mode"  # Autodetect non-numeric
    
    # Test repeated values with custom threshold
    assert choose_imputation_strategy([1, 1, 1, 2], unique_ratio_threshold=0.6) == "mode"  # Low unique ratio (0.5 < 0.6)
    
    # Test empty and None values
    assert choose_imputation_strategy([]) == "mode"
    assert choose_imputation_strategy([None, None]) == "mode"
    
    # Test with mixed types
    assert choose_imputation_strategy([1, "2", 3]) == "mode"  # Non-numeric detected

def test_compute_imputation_value():
    """Test imputation value computation function."""
    # Test mean strategy
    assert compute_imputation_value([1, 2, 3], "mean") == 2.0
    assert compute_imputation_value([1.5, 2.5, 3.5], "mean") == 2.5
    
    # Test median strategy
    assert compute_imputation_value([1, 2, 3, 4], "median") == 2.5
    assert compute_imputation_value([1, 2, 3], "median") == 2.0
    
    # Test mode strategy
    assert compute_imputation_value([1, 1, 2], "mode") == 1
    assert compute_imputation_value(["a", "a", "b"], "mode") == "a"
    
    # Test with None values
    assert compute_imputation_value([1, None, 3], "mean") == 2.0
    assert compute_imputation_value([None, None], "mean") is None
    
    # Test invalid strategy
    with pytest.raises(ValueError):
        compute_imputation_value([1, 2, 3], "invalid")

def test_fill_nulls_column():
    """Test column null filling function."""
    # Test numeric data
    values, report = fill_nulls_column([1, None, 3])
    assert values == [1, 2.0, 3]
    assert report.strategy == "mean"
    assert report.value_used == 2.0
    
    # Test categorical data
    values, report = fill_nulls_column(["a", None, "a"])
    assert values == ["a", "a", "a"]
    assert report.strategy == "mode"
    assert report.value_used == "a"
    
    # Test explicit strategy
    values, report = fill_nulls_column([1, None, 3], explicit_strategy="median")
    assert values == [1, 2, 3]
    assert report.strategy == "median"
    
    # Test all None values
    values, report = fill_nulls_column([None, None])
    assert values == [None, None]
    assert report.value_used is None

def test_fill_nulls_record():
    """Test record null filling function."""
    # Test basic record filling
    record = {"a": 1, "b": None, "c": "x"}
    samples = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["x", "y", "x"]}
    filled, reports = fill_nulls_record(record, ["a", "b", "c"], samples)
    
    assert filled["a"] == 1
    assert filled["b"] == 5.0  # Mean of samples
    assert filled["c"] == "x"
    assert len(reports) == 3
    assert all(isinstance(r, ImputationReport) for r in reports)
    
    # Test with explicit strategies
    strategies = {"b": "median"}
    filled, reports = fill_nulls_record(record, ["a", "b", "c"], samples, strategies=strategies)
    assert filled["b"] == 5.0  # Median of samples
    
    # Test with empty samples
    filled, reports = fill_nulls_record(record, ["a", "b", "c"], {})
    assert filled["b"] is None  # No samples to impute from
    
    # Test with missing columns
    filled, reports = fill_nulls_record(record, ["a", "d"], samples)
    assert "d" in filled
    assert len(reports) == 2