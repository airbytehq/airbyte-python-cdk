"""Unit tests for cleaning transforms."""
import pytest
from airbyte_cdk.utils.transforms.cleaning import (
    to_lower,
    strip_whitespace,
    squash_whitespace,
    normalize_unicode,
    remove_punctuation,
    map_values,
    cast_numeric,
)

def test_to_lower():
    """Test string lowercasing function."""
    # Test normal cases
    assert to_lower("Hello") == "hello"
    assert to_lower("HELLO") == "hello"
    assert to_lower("HeLLo") == "hello"
    
    # Test with spaces and special characters
    assert to_lower("Hello World!") == "hello world!"
    assert to_lower("Hello123") == "hello123"
    
    # Test empty and None
    assert to_lower("") == ""
    assert to_lower(None) is None

def test_strip_whitespace():
    """Test whitespace stripping function."""
    # Test normal cases
    assert strip_whitespace("  hello  ") == "hello"
    assert strip_whitespace("hello") == "hello"
    
    # Test with tabs and newlines
    assert strip_whitespace("\thello\n") == "hello"
    assert strip_whitespace("  hello\n  world  ") == "hello\n  world"
    
    # Test empty and None
    assert strip_whitespace("   ") == ""
    assert strip_whitespace("") == ""
    assert strip_whitespace(None) is None

def test_squash_whitespace():
    """Test whitespace squashing function."""
    # Test normal cases
    assert squash_whitespace("hello   world") == "hello world"
    assert squash_whitespace("  hello  world  ") == "hello world"
    
    # Test with tabs and newlines
    assert squash_whitespace("hello\n\nworld") == "hello world"
    assert squash_whitespace("hello\t\tworld") == "hello world"
    assert squash_whitespace("\n hello \t world \n") == "hello world"
    
    # Test empty and None
    assert squash_whitespace("   ") == ""
    assert squash_whitespace("") == ""
    assert squash_whitespace(None) is None

def test_normalize_unicode():
    """Test unicode normalization function."""
    # Test normal cases
    assert normalize_unicode("hello") == "hello"
    
    # Test composed characters
    assert normalize_unicode("café") == "café"  # Composed 'é'
    
    # Test decomposed characters
    decomposed = "cafe\u0301"  # 'e' with combining acute accent
    assert normalize_unicode(decomposed) == "café"  # Should normalize to composed form
    
    # Test different normalization forms
    assert normalize_unicode("café", form="NFD") != normalize_unicode("café", form="NFC")
    
    # Test empty and None
    assert normalize_unicode("") == ""
    assert normalize_unicode(None) is None

def test_remove_punctuation():
    """Test punctuation removal function."""
    # Test normal cases
    assert remove_punctuation("hello, world!") == "hello world"
    assert remove_punctuation("hello.world") == "helloworld"
    
    # Test with multiple punctuation marks
    assert remove_punctuation("hello!!! world???") == "hello world"
    assert remove_punctuation("hello@#$%world") == "helloworld"
    
    # Test with unicode punctuation
    assert remove_punctuation("hello—world") == "helloworld"
    assert remove_punctuation("«hello»") == "hello"
    
    # Test empty and None
    assert remove_punctuation("") == ""
    assert remove_punctuation(None) is None

def test_map_values():
    """Test value mapping function."""
    mapping = {"a": 1, "b": 2, "c": 3}
    
    # Test normal cases
    assert map_values("a", mapping) == 1
    assert map_values("b", mapping) == 2
    
    # Test with default value
    assert map_values("x", mapping) is None
    assert map_values("x", mapping, default=0) == 0
    
    # Test with different value types
    mixed_mapping = {1: "one", "two": 2, None: "null"}
    assert map_values(1, mixed_mapping) == "one"
    assert map_values(None, mixed_mapping) == "null"

def test_cast_numeric():
    """Test numeric casting function."""
    # Test successful casts
    assert cast_numeric("123") == 123
    assert cast_numeric("123.45") == 123.45
    assert cast_numeric(123) == 123
    assert cast_numeric(123.45) == 123.45
    
    # Test integers vs floats
    assert isinstance(cast_numeric("123"), int)
    assert isinstance(cast_numeric("123.45"), float)
    
    # Test empty values
    assert cast_numeric(None) is None
    assert cast_numeric("", on_error="none") is None  # Need to specify on_error="none" to get None for empty string
    assert cast_numeric("   ", on_error="none") is None  # Need to specify on_error="none" to get None for whitespace
    
    # Test empty values with default behavior (on_error="ignore")
    assert cast_numeric("") == ""
    assert cast_numeric("   ") == "   "
    
    # Test error handling modes
    non_numeric = "abc"
    assert cast_numeric(non_numeric, on_error="ignore") == non_numeric
    assert cast_numeric(non_numeric, on_error="none") is None
    assert cast_numeric(non_numeric, on_error="default", default=0) == 0
    
    # Test error raising
    with pytest.raises(Exception):
        cast_numeric(non_numeric, on_error="raise")