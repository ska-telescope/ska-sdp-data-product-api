"""Test for the helperfunctions methods."""

import pathlib

import pytest

from ska_sdp_dataproduct_api.core.helperfunctions import (
    filter_by_item,
    get_date_from_name,
    get_relative_path,
)
from ska_sdp_dataproduct_api.core.settings import PERSISTENT_STORAGE_PATH


# Assuming you have a logger instance in your class
class TestHelperfunctions:  # pylint: disable=R0903
    """Unit tests for the helper functions"""

    def test_get_date_from_name(self):
        """Test the get_date_from_name function."""
        # Test valid input
        assert get_date_from_name("type-generatorID-20230411-localSeq") == "2023-04-11"

        # Test invalid input (non-existent date)
        with pytest.raises(ValueError):
            get_date_from_name("type-generatorID-20231345-localSeq")

        # Test invalid input (malformed execution_block)
        with pytest.raises(IndexError):
            get_date_from_name("invalid-format")

    def test_get_relative_path(self):
        """Test the get_relative_path function."""
        # Test case 1: Absolute path within the persistent storage
        absolute_path_1 = PERSISTENT_STORAGE_PATH / pathlib.Path("file.txt")
        expected_relative_path_1 = pathlib.Path("file.txt")

        print(f"absolute_path_1: {absolute_path_1}")
        print(f"expected_relative_path_1: {expected_relative_path_1}")
        assert get_relative_path(absolute_path_1) == expected_relative_path_1

        # Test case 2: Absolute path outside the persistent storage
        absolute_path_2 = pathlib.Path("/other/path/file.txt")
        expected_relative_path_2 = pathlib.Path("/other/path/file.txt")  # No change
        assert get_relative_path(absolute_path_2) == expected_relative_path_2


def test_filter_by_item():
    """Tests for the filter_by_item method"""
    data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "Los Angeles"},
        {"name": "Charlie", "age": 30, "city": "Chicago"},
        {"name": "John", "age": 50},
    ]

    # Test contains operator
    filtered_data = filter_by_item(data, "name", "contains", "ice")
    assert filtered_data == [{"name": "Alice", "age": 30, "city": "New York"}]

    # Test equals operator
    filtered_data = filter_by_item(data, "age", "equals", 30)
    assert filtered_data == [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Charlie", "age": 30, "city": "Chicago"},
    ]

    # Test startsWith operator
    filtered_data = filter_by_item(data, "city", "startsWith", "Los")
    assert filtered_data == [{"name": "Bob", "age": 25, "city": "Los Angeles"}]

    # Test endsWith operator
    filtered_data = filter_by_item(data, "name", "endsWith", "ie")
    assert filtered_data == [{"name": "Charlie", "age": 30, "city": "Chicago"}]

    # Test isEmpty operator
    filtered_data = filter_by_item(data, "city", "isEmpty", False)
    assert filtered_data == [{"age": 50, "name": "John"}]

    # Test isNotEmpty operator
    filtered_data = filter_by_item(data, "age", "isNotEmpty", True)
    assert filtered_data == data

    # Test isAnyOf operator
    filtered_data = filter_by_item(data, "city", "isAnyOf", "New York,Chicago")
    print(filtered_data)
    assert filtered_data == [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Charlie", "age": 30, "city": "Chicago"},
    ]

    # Test unsupported operator
    with pytest.raises(ValueError) as excinfo:
        filter_by_item(data, "name", "not_supported", "value")
    assert "Unsupported filter operator: not_supported" in str(excinfo.value)
