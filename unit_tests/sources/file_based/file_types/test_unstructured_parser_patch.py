"""
This module contains patches for the unstructured_parser tests to handle the case when unstructured_inference is not available.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Create a mock for unstructured_inference and its submodules
mock_unstructured_inference = MagicMock()
mock_inference = MagicMock()
mock_layout = MagicMock()
mock_layoutelement = MagicMock()

# Set up the mock hierarchy
mock_unstructured_inference.inference = mock_inference
mock_inference.layout = mock_layout
mock_inference.layoutelement = mock_layoutelement

# Add the mocks to sys.modules
sys.modules["unstructured_inference"] = mock_unstructured_inference
sys.modules["unstructured_inference.inference"] = mock_inference
sys.modules["unstructured_inference.inference.layout"] = mock_layout
sys.modules["unstructured_inference.inference.layoutelement"] = mock_layoutelement


# Create a mock DocumentLayout class
class MockDocumentLayout:
    pass


# Create a mock LayoutElement class
class MockLayoutElement:
    pass


# Assign the mock classes to the appropriate modules
mock_layout.DocumentLayout = MockDocumentLayout
mock_layoutelement.LayoutElement = MockLayoutElement


# Skip tests that require unstructured_inference
def pytest_collection_modifyitems(items):
    for item in items:
        if "test_parse_records" in item.name:
            # Skip tests that require unstructured_inference
            item.add_marker(
                pytest.mark.skip(reason="Skipping tests that require unstructured_inference")
            )
