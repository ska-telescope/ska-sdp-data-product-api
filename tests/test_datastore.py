"""Test for the datastore methods."""

from pathlib import Path

import pytest
import yaml

from ska_sdp_dataproduct_api.components.metadatastore.datastore import (
    SearchStoreSuperClass,  # Replace with the actual import path
)
from ska_sdp_dataproduct_api.utilities.helperfunctions import FileUrl

from .test_files.example_files.expected_metadata import expected_metadata


# Assuming you have a logger instance in your class
class TestDatastore:
    """Unit tests for the datastore class"""

    def test_existing_file(self):
        """Test the exsiting file / happy path"""
        # Create a temporary file for testing
        temp_file = Path("test_file.txt")
        temp_file.touch()

        # Instantiate your class (replace with actual instantiation)
        my_instance = SearchStoreSuperClass()

        # Call the method with an existing file
        result = my_instance.check_file_exists(temp_file)

        # Clean up the temporary file
        temp_file.unlink()

        assert result is True

    def test_non_existing_file(self):
        """Test the non exsiting file / unhappy path"""
        # Instantiate your class (replace with actual instantiation)

        my_instance = SearchStoreSuperClass()

        # Call the method with a non-existing file
        result = my_instance.check_file_exists(Path("non_existent_file.txt"))

        assert result is False

    def test_load_metadata_from_file_happy_path(self):
        """
        Test loading metadata from a valid YAML file.
        """
        my_instance = SearchStoreSuperClass()
        test_metadata_file = FileUrl
        test_metadata_file.fileName = "ska-data-product.yaml"
        test_metadata_file.fullPathName = (
            "tests/test_files/product/eb-m001-20230921-245/ska-data-product.yaml"
        )

        loaded_metadata_from_file = my_instance.load_metadata_file(test_metadata_file)
        assert loaded_metadata_from_file == expected_metadata

    def test_load_metadata_from_file_file_not_found(self):
        """
        Test loading metadata from a valid YAML file.
        """
        my_instance = SearchStoreSuperClass()
        test_metadata_file = FileUrl
        test_metadata_file.fileName = "nonexistent_file_name"
        test_metadata_file.fullPathName = "/path/to/nonexistent/file.yaml"

        # Ensure that the FileNotFoundError is raised
        with pytest.raises(FileNotFoundError):
            my_instance.load_metadata_file(test_metadata_file)

    def test_load_metadata_from_file_yaml_error(self, tmp_path: Path):
        """
        Test handling YAML parsing errors.
        """
        my_instance = SearchStoreSuperClass()
        test_metadata_file = FileUrl
        test_metadata_file.fileName = "ska-data-product.yaml"
        test_metadata_file.fullPathName = tmp_path / "ska-data-product.yaml"

        # Create an invalid YAML file (e.g., missing colon)
        with open(test_metadata_file.fullPathName, "w", encoding="utf-8") as file:
            file.write("""\n    invalid_key: : value\n    """)

        with pytest.raises(yaml.YAMLError):
            my_instance.load_metadata_file(test_metadata_file)
