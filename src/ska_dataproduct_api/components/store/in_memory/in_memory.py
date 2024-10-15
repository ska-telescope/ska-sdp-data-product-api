"""
Module for in-memory volume index metadata store.

This module implements the `InMemoryVolumeIndexMetadataStore` class,
which inherits from the base `MetadataStore` class. It provides functionalities
for managing data product metadata in an in-memory store with the ability to
reindex from persistent storage.

The class offers methods to:

* Retrieve the current store status.
* Re-index the metadata store from a persistent storage location.
* List all data product files within a specified directory.
* Ingest metadata from YAML files or `DataProductMetadata` instances.
* Retrieve metadata for a specific execution block.
* Retrieve the data product file path for a given execution block.
* Check if a file exists at a specified path.

NOTE: This in-memory store is deprecated and will be removed after all users have access to
persistent PostgreSQL deployments.

"""
import logging
import pathlib
import uuid
from typing import Any

from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.store.metadata_store_base_class import MetadataStore
from ska_dataproduct_api.configuration.settings import PERSISTENT_STORAGE_PATH
from ska_dataproduct_api.utilities.helperfunctions import (
    DataProductIdentifier,
    validate_data_product_identifier,
)

logger = logging.getLogger(__name__)


class InMemoryVolumeIndexMetadataStore(MetadataStore):
    """Class to handle data ingest from various sources"""

    def __init__(self):
        super().__init__()
        self.postgresql_running: bool = False
        self.number_of_dataproducts: int = 0
        self.number_of_metadata_files: int = 0
        self.list_of_data_product_paths: list[pathlib.Path] = []
        self.dict_of_data_products_metadata: dict[DataProductMetadata] = {}
        self.reindex_persistent_volume()

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "store_type": "In memory volume index metadata store",
            "number_of_dataproducts_loaded": self.number_of_dataproducts,
            "number_of_metadata_files_found": self.number_of_metadata_files,
            "last_metadata_update_time": self.date_modified,
            "indexing": self.indexing,
        }

    def reindex_persistent_volume(self) -> None:
        """This method resets and recreates the flattened_list_of_dataproducts_metadata. This is
        added to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        try:
            logger.info("Re-indexing persistent volume store...")
            self.indexing = True
            self.list_of_data_product_paths.clear()
            self.list_of_data_product_paths: list[str] = self.list_all_data_product_files(
                PERSISTENT_STORAGE_PATH
            )
            self.number_of_metadata_files = len(self.list_of_data_product_paths)
            self.ingest_list_of_data_product_paths()
            self.update_data_store_date_modified()
            self.indexing = False
            logger.info("Metadata store re-indexed")
        except Exception as exception:
            self.indexing = False
            raise exception

    def ingest_list_of_data_product_paths(self) -> None:
        """
        This method ingests metadata files from a specified storage location into the metadata
        store.

        Args:
            full_path_name: The Path object representing the storage location containing the
            metadata files.

        Returns:
            None
        """
        for product_path in self.list_of_data_product_paths:
            try:
                _ = self.ingest_file(product_path)
            except Exception as error:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to ingest data product at file location: %s, due to error: %s",
                    str(product_path),
                    error,
                )

    def ingest_file(self, data_product_metadata_file_path: pathlib.Path) -> uuid.UUID:
        """
        Ingests a data product file by loading its metadata, structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_metadata_file_path (pathlib.Path): The path to the data file.

        Returns:
            None
        """
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
            data_product_metadata_instance.load_metadata_from_yaml_file(
                data_product_metadata_file_path
            )

        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to ingest dataproduct %s in list of products paths. Error: %s",
                data_product_metadata_file_path,
                error,
            )
            raise error

        self.dict_of_data_products_metadata[
            str(data_product_metadata_instance.data_product_uuid)
        ] = data_product_metadata_instance

        self.number_of_dataproducts = len(self.dict_of_data_products_metadata)

        return data_product_metadata_instance.data_product_uuid

    def ingest_metadata(self, metadata: dict) -> uuid.UUID:
        """
        Ingests a data product,structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_metadata_file_path (pathlib.Path): The path to the data file.

        Returns:
            None
        """
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
            data_product_metadata_instance.load_metadata_from_class(metadata)

        except Exception as error:
            logger.error(
                "Failed to ingest dataproduct metadata: %s. Error: %s",
                metadata,
                error,
            )
            raise error

        self.dict_of_data_products_metadata[
            str(data_product_metadata_instance.data_product_uuid)
        ] = data_product_metadata_instance

        self.number_of_dataproducts = len(self.dict_of_data_products_metadata)

        return data_product_metadata_instance.data_product_uuid

    def get_metadata(self, data_product_uuid: str) -> dict[str, Any]:
        """Retrieves metadata for the given uuid.

        Args:
            data_product_uuid: The data product uuid identifier.

        Returns:
            A dictionary containing the metadata for the uuid, or None if not found.
        """
        if not data_product_uuid:
            logger.warning("Metadata not found for uuid: %s", data_product_uuid)
            return {}

        try:
            return self.dict_of_data_products_metadata[data_product_uuid].metadata_dict
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Failed to get metadata for execution block, error: %s", error)
            return {}

    def get_data_product_file_paths(
        self, data_product_identifier: DataProductIdentifier
    ) -> list[pathlib.Path]:
        """Retrieves the file path to the data product for the given execution block.

        Args:
            execution_block: The execution block to retrieve metadata for.

        Returns:
            The list of file path as a pathlib.Path objects.
        """

        try:
            validate_data_product_identifier(data_product_identifier)
        except ValueError as error:
            logger.warning(
                "File path not found for data product, error: %s",
                error,
            )
            return []

        try:
            if data_product_identifier.execution_block:
                file_paths: list[pathlib.Path] = []
                for _, metadata in self.dict_of_data_products_metadata.items():
                    if metadata.execution_block == data_product_identifier.execution_block:
                        file_paths.append(pathlib.Path(metadata.metadata_dict["dataproduct_file"]))
                return file_paths
            if data_product_identifier.uuid:
                return [
                    pathlib.Path(
                        self.dict_of_data_products_metadata[
                            data_product_identifier.uuid
                        ].metadata_dict["dataproduct_file"]
                    )
                ]
            return []
        except KeyError:
            logger.warning(
                "File path not found for data product with identifier: %s",
                data_product_identifier.uuid or data_product_identifier.execution_block,
            )
            return []

    def check_file_exists(self, file_object: pathlib.Path) -> bool:
        """
        Checks if the given file path points to an existing file.

        Args:
            file_object (pathlib.Path): The full path to the file.

        Returns:
            Bool: True if the file exists, otherwise False.
        """
        if not file_object.is_file():
            logger.warning(
                "Metadata file path '%s' not pointing to a file.",
                str(file_object),
            )
            return False
        return True
