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
    Encapsulates metadata for a data product, providing access to its properties and location.

    This class manages metadata associated with a data product, including file paths,
    unique identifiers, execution context, and data storage information. It is designed
    to facilitate the consistent handling and retrieval of metadata related to data products.

    Attributes:
        data_product_file_path (pathlib.Path): Path to the actual data product file.
        data_product_metadata_file_path (pathlib.Path): Path to the metadata file associated with
            the data product.
        metadata_dict (dict): Loaded metadata as a dictionary, representing the parsed content of
            the metadata file.
        data_product_uid (uuid.UUID): Unique identifier (UUID) assigned to the data product.
        execution_block (str): Identifier or name of the execution block that generated
            the data product.
        metadata_dict_hash (str): Hash value of the metadata dictionary, used for integrity checks
            or versioning.
        data_store (str): Name of the data store where the data product is located
            (defaults to "dpd").

    Args:
        data_store (str, optional): The data store identifier. Defaults to "dpd".
    """

    def __init__(self, data_store: str = "dpd"):
        self.data_product_file_path: pathlib.Path = None
        self.data_product_metadata_file_path: pathlib.Path = None
        self.metadata_dict: dict = None
        self.data_product_uid: uuid.UUID = None
        self.execution_block: str = None
        self.metadata_dict_hash: str = None
        self.data_store: str = data_store

    def appended_metadata_dict(
        self,
    ) -> dict:
        """
        Combines the existing metadata dictionary with additional metadata fields.

        This method creates a copy of the instance's `metadata_dict` and adds
        the following fields:
        - "date_created": The creation date and time of the data product.
        - "dataproduct_file": The file path of the data product.
        - "metadata_file": The file path of the data product's metadata file.
        - "data_store": Name of the data store where the data product is located.
        - "uid": The unique identifier of the data product.

        Returns:
            dict: A dictionary containing the combined metadata.

        """
        combined_dict = self.metadata_dict.copy()
        combined_dict.update(
            {
                "date_created": self.get_date_from_name(self.execution_block),
                "dataproduct_file": str(self.data_product_file_path),
                "metadata_file": str(self.data_product_metadata_file_path),
                "data_store": self.data_store,
                "uid": str(self.data_product_uid),
            }
        )
        return combined_dict

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

    def derive_uid(self, execution_block_id: str, file_path: pathlib.Path) -> uuid.UUID:
        """Derives a UUID from an execution block ID and file path.

        Args:
            execution_block_id (str): Name of the execution block that generated the data product.
            file_path (pathlib.Path): The file path where the product of this execution block is
                saved.

        Returns:
            uuid.UUID: The derived UUID.

        Raises:
            ValueError: If the execution block ID is None or the UUID cannot be created.
        """
        if execution_block_id is None:
            logger.error("Execution block ID cannot be None.")
            raise ValueError("Execution block ID cannot be None.")

        # I am using a combination of the execution_block_id and file_path of the data products
        # to derive a uid. The file path would be unique and consistent for the initial use as the
        # DPD only loads data products from one PV.

        # This is also not envisioned to be the source of the global UUID of data products, it
        # should only be the DLM creating and assigning these uid's, but for internal use for the
        # DPD, to cater for data products that is loaded by the DPD from the PV or for sub
        # products, we assign a local uid here.

        try:
            combined_string = f"{execution_block_id}:{str(file_path)}"
            hash_value = hashlib.sha256(combined_string.encode("utf-8")).hexdigest()
            formatted_hash = f"{hash_value[:8]}-{hash_value[8:12]}-{hash_value[12:16]}-\
{hash_value[16:20]}-{hash_value[20:32]}"
            uid_value = uuid.UUID(formatted_hash)
            return uid_value
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
            file_path (pathlib.Path): Path to the YAML file containing metadata.

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
        self.data_product_uid = self.derive_uid(
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
        return self.metadata_dict

    def load_metadata_from_class(self, metadata: dict, dlm_uid: uuid = None) -> dict[str, any]:
        """
        Loads metadata from a dict.

        Args:
            metadata (dict): The dict instance containing the metadata.
            dlm_uid (uuid.UUID): The unique identifier of the data product in the DLM
                (default = None).

        Returns:
            A dictionary containing the loaded metadata.
        """
        self.metadata_dict = metadata
        self.execution_block = self.get_execution_block_id(self.metadata_dict)
        self.metadata_dict_hash = self.calculate_metadata_hash(self.metadata_dict)
        if self.data_store == "dlm":
            self.data_product_uid = dlm_uid
        else:
            self.data_product_uid = self.derive_uid(
                execution_block_id=self.execution_block, file_path=self.data_product_file_path
            )
        return self.metadata_dict

    def get_date_from_name(self, execution_block: str) -> str:
        """
        Extracts a date string from an execution block (type-generatorID-datetime-localSeq from
        https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers) and converts it
        to the format 'YYYY-MM-DD'.
        If the function fails to derive a valid date, it will return 1970-01-01 as a default.

        Args:
            execution_block (str): A string containing metadata information.

        Returns:
            str: The formatted date string in 'YYYY-MM-DD' format.

        Example:
            >>> get_date_from_name("type-generatorID-20230411-localSeq")
            '2023-04-11'
        """
        try:
            metadata_date_str = execution_block.split("-")[2]
            date_obj = datetime.datetime.strptime(metadata_date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except (ValueError, IndexError) as error:
            logger.error(
                "The execution_block: %s is missing or not in the following format: "
                "type-generatorID-datetime-localSeq. Error: %s",
                execution_block,
                error,
            )
            # Opting to add the computing epoch as a fallback date for products that have
            # malformed execution_block id's. This allows them to be displayed on the DPD.
            date_obj = datetime.datetime.strptime("19700101", "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
