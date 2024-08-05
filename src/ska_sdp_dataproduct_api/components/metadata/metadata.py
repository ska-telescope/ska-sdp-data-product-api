"""
Module for handling data product metadata.

This module provides a class for encapsulating and managing metadata associated with data products.
It offers functionalities to load metadata from YAML files, append additional metadata, and extract
relevant information.

Classes:
    DataProductMetadata: Encapsulates metadata for a data product.

Functions:
    None
"""

import logging
import pathlib

import yaml

from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    DataProductMetaData,
    get_date_from_name,
)

logger = logging.getLogger(__name__)


class DataProductMetadata:
    """
    Encapsulates metadata for a data product.

    Attributes:
        data_product_file_path (pathlib.Path): Path to the metadata file.
        metadata_dict (dict): Loaded metadata as a dictionary.
    """

    def __init__(self):
        self.data_product_file_path: pathlib.Path = None
        self.data_product_metadata_file_path: pathlib.Path = None
        self.metadata_dict: dict = None
        self.date_created: str = None

    def load_yaml_file(self, file_path: pathlib.Path) -> dict:
        """
        Loads metadata from a YAML file.

        Args:
            data_product_file_path (pathlib.Path): Path to the metadata file.

        Returns:
            dict: Loaded metadata as a dictionary.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            yaml.YAMLError: If there's an error parsing the YAML file.
        """
        self.data_product_metadata_file_path = file_path
        self.data_product_file_path = self.data_product_metadata_file_path.parent

        try:
            with open(self.data_product_metadata_file_path, "r", encoding="utf-8") as file:
                self.metadata_dict = yaml.safe_load(file)
            return self.metadata_dict
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"Metadata file not found: {self.data_product_metadata_file_path}"
            ) from error
        except yaml.YAMLError as error:
            raise yaml.YAMLError(
                f"Error parsing YAML file: {self.data_product_metadata_file_path}"
            ) from error

    def load_metadata_from_yaml_file(self, file_path: pathlib.Path) -> dict[str, any]:
        """Loads metadata from a DataProductMetaData class.

        Args:
            metadata: The DataProductMetaData instance containing the metadata.

        Returns:
            A dictionary containing the loaded metadata.
        """
        data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
        data_product_metadata_instance.load_yaml_file(file_path=file_path)
        self.append_metadata()
        return self.metadata_dict

    def load_metadata_from_class(self, metadata: DataProductMetaData) -> dict[str, any]:
        """Loads metadata from a DataProductMetaData class.

        Args:
            metadata: The DataProductMetaData instance containing the metadata.

        Returns:
            A dictionary containing the loaded metadata.
        """
        self.metadata_dict = metadata.dict()
        self.append_metadata()
        return self.metadata_dict

    def append_metadata(self) -> None:
        """Appends metadata to the object.

        This method attempts to get the date from metadata and append file details.
        Any exceptions encountered during the process are logged.

        Raises:
            Exception: If an error occurs during metadata appending.
        """

        try:
            self.get_date_from_metadata()
            self.append_metadata_file_details()
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Failed to append metadata, error: %s", error)

    def get_date_from_metadata(self) -> None:
        """Extracts the date from the metadata and assigns it to self.date_created.

        Attempts to extract the date from the 'execution_block' key in the metadata dictionary.
        If an error occurs, logs an error message and does not set the date.

        Args:
            self: The object instance.
        """

        try:
            self.date_created = get_date_from_name(self.metadata_dict["execution_block"])
        except (KeyError, ValueError, TypeError) as exception:
            logger.error(
                "Failed to extract date from execution block: %s. Error: %s",
                self.metadata_dict.get("execution_block", "Unknown"),
                exception,
            )

    def append_metadata_file_details(self) -> None:
        """Appends metadata file details to the metadata dictionary.

        Updates the metadata dictionary with the following keys:
        - 'date_created': The date the metadata was created.
        - 'dataproduct_file': The path to the data product file as a string.
        - 'metadata_file': The path to the metadata file as a string.
        """

        self.metadata_dict.update(
            {
                "date_created": self.date_created,
                "dataproduct_file": str(
                    self.data_product_file_path
                ),  # TODO This file path needs to be updated in case where not loaded from file
                "metadata_file": str(
                    self.data_product_metadata_file_path
                ),  # TODO This file path needs to be updated in case where not loaded form file
            }
        )
