"""Unit tests for math transforms."""
import math
import pytest
from airbyte_cdk.utils.transforms.math import (
    minmax_scale,
    zscore,
    clip,
    winsorize,
    log1p_safe,
    bucketize,
    robust_percentile_scale,
)

def test_minmax_scale():
    """Test minmax scaling function."""
    # Test normal scaling
    assert minmax_scale(5, 0, 10) == 0.5
    assert minmax_scale(5, 0, 10, (0, 100)) == 50.0
    
    # Test edge cases
    assert minmax_scale(0, 0, 10) == 0.0
    assert minmax_scale(10, 0, 10) == 1.0
    
    # Test custom range scaling
    assert minmax_scale(5, 0, 10, (-1, 1)) == 0.0
    
    # Test when data_max equals data_min (prevents division by zero)
    assert minmax_scale(5, 5, 5) == 0.5  # Should return middle of output range
    
    # Test with float inputs
    assert minmax_scale(5.5, 0.0, 10.0) == 0.55

def test_zscore():
    """Test z-score calculation function."""
    # Test normal cases
    assert zscore(10, 5, 2) == 2.5  # (10 - 5) / 2
    assert zscore(0, 5, 2) == -2.5  # (0 - 5) / 2
    
    # Test with zero sigma
    assert zscore(10, 5, 0) == 0.0  # Should handle division by zero gracefully
    
    # Test with float inputs
    assert zscore(10.5, 5.0, 2.0) == 2.75

def test_clip():
    """Test value clipping function."""
    # Test normal clipping
    assert clip(5, 0, 10) == 5
    assert clip(-1, 0, 10) == 0
    assert clip(11, 0, 10) == 10
    
    # Test with float values
    assert clip(5.5, 0.0, 10.0) == 5.5
    assert clip(-1.5, 0.0, 10.0) == 0.0
    
    # Test when low == high
    assert clip(5, 3, 3) == 3

def test_winsorize():
    """Test winsorization function."""
    # Test normal cases
    assert winsorize(5, 0, 10) == 5
    assert winsorize(-1, 0, 10) == 0
    assert winsorize(11, 0, 10) == 10
    
    # Test with float values
    assert winsorize(5.5, 0.0, 10.0) == 5.5
    
    # Test when low == high
    assert winsorize(5, 3, 3) == 3

def test_log1p_safe():
    """Test safe log1p calculation function."""
    # Test normal cases
    assert log1p_safe(0) == 0.0
    assert log1p_safe(math.e - 1) == 1.0
    
    # Test negative values > -1
    assert abs(log1p_safe(-0.5) - math.log1p(-0.5)) < 1e-10
    
    # Test negative values <= -1
    assert log1p_safe(-2) == -2.0  # Should return input value
    
    # Test error cases
    assert log1p_safe(float('inf')) == float('inf')

def test_bucketize():
    """Test bucketization function."""
    edges = [0, 10, 20, 30]
    
    # Test normal cases
    assert bucketize(-5, edges) == 0
    assert bucketize(5, edges) == 1
    assert bucketize(15, edges) == 2
    assert bucketize(25, edges) == 3
    assert bucketize(35, edges) == 4
    
    # Test edge values
    assert bucketize(0, edges) == 0
    assert bucketize(10, edges) == 1
    assert bucketize(20, edges) == 2
    assert bucketize(30, edges) == 3
    
    # Test empty edges
    assert bucketize(5, []) == 0
    
        # Test single edge
    assert bucketize(5, [10]) == 0  # 5 ≤ 10, so bucket 0
    assert bucketize(15, [10]) == 1  # 15 > 10, so bucket 1

def test_robust_percentile_scale():
    """Test robust percentile scaling function."""
    # Test normal scaling
    assert robust_percentile_scale(5, 0, 10) == 0.5
    assert robust_percentile_scale(5, 0, 10, (0, 100)) == 50.0
    
    # Test edge cases
    assert robust_percentile_scale(0, 0, 10) == 0.0
    assert robust_percentile_scale(10, 0, 10) == 1.0
    
    # Test custom range
    assert robust_percentile_scale(5, 0, 10, (-1, 1)) == 0.0
    
    # Test clipping
    assert robust_percentile_scale(-1, 0, 10) == 0.0  # With clipping
    assert robust_percentile_scale(-1, 0, 10, clip_outliers=False) < 0.0  # Without clipping
    
    # Test when high equals low
    assert robust_percentile_scale(5, 5, 5) == 0.5  # Should return middle of output range
