"""Test for the helperfunctions methods."""

import pytest

from ska_sdp_dataproduct_api.core.helperfunctions import get_date_from_name


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
