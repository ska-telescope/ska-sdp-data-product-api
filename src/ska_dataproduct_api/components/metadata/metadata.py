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

import datetime
import hashlib
import json
import logging
import pathlib
import uuid

import yaml

logger = logging.getLogger(__name__)

# pylint: disable=too-many-instance-attributes


class DataProductMetadata:
    """
    Encapsulates metadata for a data product.

    Attributes:
        data_product_file_path (pathlib.Path): Path to the data product file.
        data_product_metadata_file_path (pathlib.Path): Path to the metadata file.
        metadata_dict (dict): Loaded metadata as a dictionary.
        date_created (str): Date when the metadata was created.
    """

    def __init__(self):
        self.data_product_file_path: pathlib.Path = None
        self.data_product_metadata_file_path: pathlib.Path = None
        self.metadata_dict: dict = None
        self.date_created: str = None
        self.object_id: str = None
        self.data_product_uuid: uuid.UUID = None
        self.execution_block: str = None
        self.metadata_dict_hash: str = None

    def get_execution_block_id(self, metadata_dict: dict) -> str | None:
        """Retrieves the execution block ID from the given metadata dictionary.

        Args:
            metadata_dict (dict): The metadata dictionary.

        Returns:
            str | None: The execution block ID, or None if not found.

        Raises:
            KeyError: If the `execution_block` key is not found in the metadata dictionary.
        """

        try:
            return metadata_dict["execution_block"]
        except KeyError as error:
            logger.error("execution_block value key not found in data product, error: %s", error)
            raise error

    def derive_uuid(self, execution_block_id: str, file_path: pathlib.Path) -> uuid.UUID:
        """Derives a UUID from an execution block ID and file path.

        Args:
            execution_block_id (str): The execution block ID.
            file_path (pathlib.Path): The file path.

        Returns:
            uuid.UUID: The derived UUID.

        Raises:
            ValueError: If the execution block ID is None.
            ValueError: If the UUID cannot be created.
        """
        if execution_block_id is None:
            logger.error("Execution block ID cannot be None.")
            raise ValueError("Execution block ID cannot be None.")

        # I am using a combination of the execution_block_id and file_path of the data products
        # to derive a uuid. The file path would be unique and consistent for the initial use as the
        # DPD only loads data products from one PV, but as soon as the DLM adds data products
        # to the dashboard, there might not be a direct reference to a file on disk.

        # This is also not envisioned to be the source of the global UUID of data products, it
        # should only be the DLM creating and assigning these uuid's, but for internal use for the
        # DPD, to cater for data products that is loaded by the DPD from the PV or for sub
        # products, we assign a local uuid here.

        try:
            combined_string = f"{execution_block_id}:{str(file_path)}"
            hash_value = hashlib.sha256(combined_string.encode("utf-8")).hexdigest()
            formatted_hash = f"{hash_value[:8]}-{hash_value[8:12]}-{hash_value[12:16]}-\
{hash_value[16:20]}-{hash_value[20:32]}"
            uuid_value = uuid.UUID(formatted_hash)
            return uuid_value
        except ValueError as error:
            logger.error("Failed to create UUID: %s", error)
            raise error

    def calculate_metadata_hash(self, metadata_file_json: dict) -> str:
        """Calculates a SHA256 hash of the given metadata JSON.

        Args:
            metadata_file_json (dict): The metadata JSON to be hashed.

        Returns:
            str: The SHA256 hash of the metadata JSON.
        """

        return hashlib.sha256(json.dumps(metadata_file_json).encode("utf-8")).hexdigest()

    def load_yaml_file(self, file_path: pathlib.Path) -> None:
        """
        Loads metadata from a YAML file.

        Args:
            data_product_file_path (pathlib.Path): Path to the metadata file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            yaml.YAMLError: If there's an error parsing the YAML file.
        """
        self.data_product_metadata_file_path = file_path
        self.data_product_file_path = self.data_product_metadata_file_path.parent

        try:
            with open(self.data_product_metadata_file_path, "r", encoding="utf-8") as file:
                self.metadata_dict = yaml.safe_load(file)
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"Metadata file not found: {self.data_product_metadata_file_path}"
            ) from error
        except yaml.YAMLError as error:
            raise yaml.YAMLError(
                f"Error parsing YAML file: {self.data_product_metadata_file_path}"
            ) from error

        self.execution_block = self.get_execution_block_id(self.metadata_dict)
        self.metadata_dict_hash = self.calculate_metadata_hash(self.metadata_dict)
        self.data_product_uuid = self.derive_uuid(
            execution_block_id=self.execution_block, file_path=self.data_product_file_path
        )

    def load_metadata_from_yaml_file(self, file_path: pathlib.Path) -> dict[str, any]:
        """
        Loads metadata from a YAML file into the object.

        Args:
            file_path (pathlib.Path): Path to the YAML file containing metadata.

        Returns:
            dict[str, any]: Loaded metadata as a dictionary.
        """
        try:
            self.load_yaml_file(file_path=file_path)
        except Exception as error:
            logger.error("Failed to load metadata, error: %s", error)
            raise error

        self.append_metadata()
        return self.metadata_dict

    def load_metadata_from_class(self, metadata: dict) -> dict[str, any]:
        """
        Loads metadata from a dict.

        Args:
            metadata: The dict instance containing the metadata.

        Returns:
            A dictionary containing the loaded metadata.
        """
        self.metadata_dict = metadata
        self.execution_block = self.get_execution_block_id(self.metadata_dict)
        self.metadata_dict_hash = self.calculate_metadata_hash(self.metadata_dict)
        self.data_product_uuid = self.derive_uuid(
            execution_block_id=self.execution_block, file_path=self.data_product_file_path
        )
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
        """

        try:
            self.date_created = self.get_date_from_name(self.execution_block)
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
                "dataproduct_file": str(self.data_product_file_path),
                "metadata_file": str(self.data_product_metadata_file_path),
                "uuid": str(self.data_product_uuid),
            }
        )

    def get_date_from_name(self, execution_block: str) -> str:
        """
        Extracts a date string from an execution block (type-generatorID-datetime-localSeq from
        https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers) and converts it
        to the format 'YYYY-MM-DD'.

        Args:
            execution_block (str): A string containing metadata information.

        Returns:
            str: The formatted date string in 'YYYY-MM-DD' format.

        Raises:
            ValueError: If the date cannot be parsed from the execution block.

        Example:
            >>> get_date_from_name("type-generatorID-20230411-localSeq")
            '2023-04-11'
        """
        try:
            metadata_date_str = execution_block.split("-")[2]
            date_obj = datetime.datetime.strptime(metadata_date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError as error:
            logger.error(
                "The execution_block: %s is missing or not in the following format: "
                "type-generatorID-datetime-localSeq. Error: %s",
                execution_block,
                error,
            )
            raise
