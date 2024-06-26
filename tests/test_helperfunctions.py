"""Test for the helperfunctions methods."""

import pathlib

import pytest

from ska_sdp_dataproduct_api.core.helperfunctions import get_date_from_name, get_relative_path
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
