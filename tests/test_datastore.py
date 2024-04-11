"""Basic test for the datastore methods."""

from pathlib import Path

from ska_sdp_dataproduct_api.core.helperfunctions import DPDAPIStatus
from ska_sdp_dataproduct_api.metadatastore.datastore import (
    Store,  # Replace with the actual import path
)


# Assuming you have a logger instance in your class
class TestCheckFileExists:
    """Unit test for the check_file_exists method"""

    def test_existing_file(self):
        """Test the exsiting file / happy path"""
        # Create a temporary file for testing
        temp_file = Path("test_file.txt")
        temp_file.touch()
        dpd_api_status = DPDAPIStatus()

        # Instantiate your class (replace with actual instantiation)
        my_instance = Store(dpd_api_status)

        # Call the method with an existing file
        result = my_instance.check_file_exists(temp_file)

        # Clean up the temporary file
        temp_file.unlink()

        assert result is True

    def test_non_existing_file(self):
        """Test the non exsiting file / unhappy path"""
        # Instantiate your class (replace with actual instantiation)
        dpd_api_status = DPDAPIStatus()

        my_instance = Store(dpd_api_status)

        # Call the method with a non-existing file
        result = my_instance.check_file_exists(Path("non_existent_file.txt"))

        assert result is False
