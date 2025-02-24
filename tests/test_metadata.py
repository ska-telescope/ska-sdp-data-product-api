"""Test for the metadata.py methods."""

import pathlib

import pytest
import yaml

from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata

data_product_metadata_instance: DataProductMetadata = DataProductMetadata(data_source="dpd")


class TestHelperfunctions:  # pylint: disable=R0903
    """Unit tests for the helper functions"""

    def test_get_date_from_name(self):
        """Test the get_date_from_name function."""
        # Test valid input
        assert (
            data_product_metadata_instance.get_date_from_name("type-generatorID-20230411-localSeq")
            == "2023-04-11"
        )

        # Test invalid input (non-existent date)
        with pytest.raises(ValueError):
            data_product_metadata_instance.get_date_from_name("type-generatorID-20231345-localSeq")

        # Test invalid input (malformed execution_block)
        with pytest.raises(IndexError):
            data_product_metadata_instance.get_date_from_name("invalid-format")


def test_load_yaml_file_file_not_found():
    """Tests if a FileNotFoundError is raised when the file doesn't exist."""
    test_file_path = pathlib.Path("non_existent_file.yaml")
    data_product_metadata = DataProductMetadata(data_source="dpd")
    with pytest.raises(FileNotFoundError) as excinfo:
        data_product_metadata.load_yaml_file(test_file_path)
    assert str(excinfo.value) == f"Metadata file not found: {test_file_path}"


def test_load_yaml_file_invalid_yaml():
    """Tests if a YAMLError is raised when the YAML file is invalid."""
    test_file_path = pathlib.Path("invalid_yaml.yaml")
    with open(test_file_path, "w", encoding="utf-8") as invalid_yaml_file:
        invalid_yaml_file.write(
            """
a: 1
b: [2, 3, 4
"""
        )
    data_product_metadata = DataProductMetadata(data_source="dpd")
    with pytest.raises(yaml.YAMLError) as excinfo:
        data_product_metadata.load_yaml_file(test_file_path)
        assert (
            data_product_metadata.metadata_dict is None
        ), "Metadata should be None for invalid YAML"
    assert str(excinfo.value) == f"Error parsing YAML file: {test_file_path}"
    test_file_path.unlink()


# calculate_metadata_hash tests
def test_calculate_metadata_hash():
    """Tests the calculation of a metadata hash."""
    # Test with a simple JSON object
    metadata_json = {
        "key1": "value1",
        "key2": [1, 2, 3],
        "key3": {"nested_key": "nested_value"},
    }
    expected_hash = "3f8ac9cedca91c2556da09a4448bf629fd3b3049e07c4d7220e76b8e12542d33"

    actual_hash = data_product_metadata_instance.calculate_metadata_hash(metadata_json)
    assert actual_hash == expected_hash

    # Test with an empty JSON object
    metadata_json = {}
    expected_hash = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"

    actual_hash = data_product_metadata_instance.calculate_metadata_hash(metadata_json)
    assert actual_hash == expected_hash
