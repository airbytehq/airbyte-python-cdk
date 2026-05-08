"""Unit tests for date transforms."""
from datetime import datetime

from airbyte_cdk.utils.transforms.date import (
    try_parse_date,
    extract_date_parts,
    floor_to_month,
    ceil_to_month,
)

def test_try_parse_date():
    """Test date parsing function."""
    # Test with datetime object
    dt = datetime(2023, 1, 15)
    assert try_parse_date(dt) == dt
    
    # Test with non-date object
    assert try_parse_date("2023-01-15") is None
    assert try_parse_date(123) is None
    assert try_parse_date(None) is None

def test_extract_date_parts():
    """Test date parts extraction function."""
    # Test with valid datetime
    dt = datetime(2023, 1, 15)  # Sunday
    parts = extract_date_parts(dt)
    assert parts["year"] == 2023
    assert parts["month"] == 1
    assert parts["day"] == 15
    assert parts["dow"] == 6  # Sunday is 6
    
    # Test with invalid input
    parts = extract_date_parts(None)
    assert all(v is None for v in parts.values())
    
    parts = extract_date_parts("not a date")
    assert all(v is None for v in parts.values())

def test_floor_to_month():
    """Test floor to month function."""
    # Test normal cases
    dt = datetime(2023, 1, 15)
    assert floor_to_month(dt) == datetime(2023, 1, 1)
    
    dt = datetime(2023, 12, 31)
    assert floor_to_month(dt) == datetime(2023, 12, 1)
    
    # Test first day of month
    dt = datetime(2023, 1, 1)
    assert floor_to_month(dt) == dt
    
    # Test with invalid input
    assert floor_to_month(None) is None
    assert floor_to_month("not a date") is None

def test_ceil_to_month():
    """Test ceil to month function."""
    # Test normal cases
    dt = datetime(2023, 1, 15)
    assert ceil_to_month(dt) == datetime(2023, 2, 1)
    
    # Test end of year
    dt = datetime(2023, 12, 15)
    assert ceil_to_month(dt) == datetime(2024, 1, 1)
    
    # Test first day of month
    dt = datetime(2023, 1, 1)
    assert ceil_to_month(dt) == datetime(2023, 2, 1)
    
    # Test with invalid input
    assert ceil_to_month(None) is None
    assert ceil_to_month("not a date") is None