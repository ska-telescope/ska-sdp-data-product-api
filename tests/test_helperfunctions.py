"""Test for the helperfunctions methods."""

import os
import pathlib
from datetime import datetime

import pytest

from ska_dataproduct_api.configuration.settings import PERSISTENT_STORAGE_PATH
from ska_dataproduct_api.utilities.helperfunctions import (
    filter_by_item,
    filter_by_key_value_pair,
    get_relative_path,
    parse_valid_date,
    walk_folder,
)


# Assuming you have a logger instance in your class
class TestHelperfunctions:  # pylint: disable=R0903
    """Unit tests for the helper functions"""

    def test_get_relative_path(self):
        """Test the get_relative_path function."""
        # Test case 1: Absolute path within the persistent storage
        absolute_path_1 = PERSISTENT_STORAGE_PATH / pathlib.Path("file.txt")
        expected_relative_path_1 = pathlib.Path("file.txt")

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

    # Test isAnyOf operator
    filtered_data = filter_by_item(data, "city", "isAnyOf", "New York,Chicago")
    assert filtered_data == [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Charlie", "age": 30, "city": "Chicago"},
    ]


def test_parse_valid_date_success():
    """Tests that the parse_valid_date function successfully parses a valid date string."""

    valid_date_string = "2024-07-02"
    expected_format = "%Y-%m-%d"
    expected_datetime = datetime(year=2024, month=7, day=2)

    parsed_datetime = parse_valid_date(valid_date_string, expected_format)

    assert parsed_datetime == expected_datetime


def test_parse_valid_date_invalid_format():
    """Tests that the parse_valid_date function raises a ValueError for an invalid format."""

    invalid_date_string = "02-07-2024"  # Incorrect format order
    expected_format = "%Y-%m-%d"  # Correct format

    with pytest.raises(ValueError) as excinfo:
        parse_valid_date(invalid_date_string, expected_format)

    assert "does not match format" in str(excinfo.value)


def test_parse_valid_date_invalid_date():
    """Tests that the parse_valid_date function raises a ValueError for an invalid date."""

    invalid_date_string = "2024-13-02"  # Invalid month
    expected_format = "%Y-%m-%d"

    with pytest.raises(ValueError) as excinfo:
        parse_valid_date(invalid_date_string, expected_format)

    assert "time data '2024-13-02' does not match format '%Y-%m-%d'" in str(excinfo.value)


def test_filter_by_key_value_pair_empty_data():
    """Tests the function with empty data list."""
    data = []
    key_value_pairs = [{"keyPair": "name", "valuePair": "Alice"}]
    expected_output = []

    assert filter_by_key_value_pair(data, key_value_pairs) == expected_output


def test_filter_by_key_value_pair_single_match():
    """Tests the function with a single matching element."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    key_value_pairs = [{"keyPair": "name", "valuePair": "Alice"}]
    expected_output = [{"name": "Alice", "age": 30}]

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_filter_by_key_value_pair_multiple_matches():
    """Tests the function with multiple matching elements."""
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Alice", "city": "New York"},
        {"name": "Bob", "age": 25},
    ]
    key_value_pairs = [{"keyPair": "name", "valuePair": "Alice"}]
    expected_output = [{"name": "Alice", "age": 30}, {"name": "Alice", "city": "New York"}]

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_filter_by_key_value_pair_no_match():
    """Tests the function with no matching elements."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    key_value_pairs = [{"keyPair": "city", "valuePair": "New York"}]
    expected_output = []

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_filter_by_key_value_pair_missing_key():
    """Tests the function with a missing key in key_value_pair."""
    data = [{"name": "Alice", "age": 30}]
    key_value_pairs = [{"valuePair": "Alice"}]  # missing "keyPair"
    expected_output = [{"age": 30, "name": "Alice"}]

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_filter_by_key_value_pair_missing_value():
    """Tests the function with a missing value in key_value_pair."""
    data = [{"name": "Alice", "age": 30}]
    key_value_pairs = [{"keyPair": "name"}]  # missing "valuePair"
    expected_output = [{"age": 30, "name": "Alice"}]

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_filter_by_key_value_pair_nested_match():
    """Tests the function with a nested matching element."""
    data = [
        {
            "name": "Alice",
            "age": 30,
            "files": [{"name": "Bob", "age": 25}, {"name": "Zuma", "age": 99}],
        }
    ]
    key_value_pairs = [{"keyPair": "name", "valuePair": "Zuma"}]
    expected_output = [
        {
            "name": "Alice",
            "age": 30,
            "files": [{"name": "Bob", "age": 25}, {"name": "Zuma", "age": 99}],
        }
    ]

    assert filter_by_key_value_pair(data.copy(), key_value_pairs) == expected_output


def test_walk_folder_empty_directory():
    """Test walking an empty directory."""
    temp_dir = "temp_test_dir"
    os.makedirs(temp_dir)
    file_paths = list(walk_folder(temp_dir))
    assert len(file_paths) == 0
    os.rmdir(temp_dir)


def test_walk_folder_single_file():
    """Test walking a directory with a single file."""
    temp_dir = "temp_test_dir"
    os.makedirs(temp_dir)
    with open(os.path.join(temp_dir, "test_file.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    file_paths = list(walk_folder(temp_dir))
    assert len(file_paths) == 1
    assert file_paths[0] == os.path.join(temp_dir, "test_file.txt")
    os.remove(os.path.join(temp_dir, "test_file.txt"))
    os.rmdir(temp_dir)


def test_walk_folder_nested_directories():
    """Test walking a directory with nested directories and files."""
    temp_dir = "temp_test_dir"
    os.makedirs(os.path.join(temp_dir, "subdir1"))
    os.makedirs(os.path.join(temp_dir, "subdir2"))
    with open(os.path.join(temp_dir, "file1.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    with open(os.path.join(temp_dir, "subdir1", "file2.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    with open(os.path.join(temp_dir, "subdir2", "file3.txt"), "w", encoding="utf-8") as f:
        f.write("test")
    file_paths = list(walk_folder(temp_dir))
    assert len(file_paths) == 3
    assert os.path.join(temp_dir, "file1.txt") in file_paths
    assert os.path.join(temp_dir, "subdir1", "file2.txt") in file_paths
    assert os.path.join(temp_dir, "subdir2", "file3.txt") in file_paths
    os.remove(os.path.join(temp_dir, "file1.txt"))
    os.remove(os.path.join(temp_dir, "subdir1", "file2.txt"))
    os.remove(os.path.join(temp_dir, "subdir2", "file3.txt"))
    os.rmdir(os.path.join(temp_dir, "subdir1"))
    os.rmdir(os.path.join(temp_dir, "subdir2"))
    os.rmdir(temp_dir)
