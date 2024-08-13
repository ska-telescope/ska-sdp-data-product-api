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
from typing import Any

from ska_sdp_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_sdp_dataproduct_api.components.store.metadata_store_base_class import MetadataStore
from ska_sdp_dataproduct_api.configuration.settings import (
    METADATA_FILE_NAME,
    PERSISTENT_STORAGE_PATH,
)
from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    DataProductMetaData,
    verify_persistent_storage_file_path,
)

logger = logging.getLogger(__name__)


class InMemoryVolumeIndexMetadataStore(MetadataStore):
    """Class to handle data ingest from various sources"""

    def __init__(self):
        super().__init__()
        self.postgresql_running: bool = False
        self.number_of_dataproducts: int = 0
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
            "number_of_dataproducts": self.number_of_dataproducts,
        }

    def reindex_persistent_volume(self) -> None:
        """This method resets and recreates the flattened_list_of_dataproducts_metadata. This is
        added to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        try:
            logger.info("Re-indexing persistent volume store...")
            self.indexing = True
            self.number_of_dataproducts = 0
            self.list_all_data_product_files(PERSISTENT_STORAGE_PATH)
            self.ingest_list_of_data_product_paths()
            self.update_data_store_date_modified()
            self.indexing = False
            logger.info("Metadata store re-indexed")
        except Exception as exception:
            self.indexing = False
            raise exception

    def list_all_data_product_files(self, full_path_name: pathlib.Path) -> None:
        """
        Lists all data product files within the specified directory path.

        This method recursively traverses the directory structure starting at `full_path_name`
        and identifies files that are considered data products based on pre-defined criteria
        of the folder containing a metadata file.

        Args:
            full_path_name (pathlib.Path): The path to the directory containing data products.

        Returns:
            None

        Raises:
            ValueError: If `full_path_name` does not represent a valid directory or is a symbolic
            link.
        """

        if not verify_persistent_storage_file_path(full_path_name):
            return
        logger.info("Identifying data product files within directory: %s", full_path_name)

        self.list_of_data_product_paths.clear()
        for file_path in PERSISTENT_STORAGE_PATH.rglob(METADATA_FILE_NAME):
            if file_path not in self.list_of_data_product_paths:
                self.list_of_data_product_paths.append(file_path)

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
            self.ingest_file(product_path)

    def ingest_file(self, data_product_metadata_file_path: pathlib.Path) -> None:
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

            self.dict_of_data_products_metadata[
                data_product_metadata_instance.metadata_dict["execution_block"]
            ] = data_product_metadata_instance
            self.number_of_dataproducts = self.number_of_dataproducts + 1

        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to ingest dataproduct %s in list of products paths. Error: %s",
                data_product_metadata_file_path,
                error,
            )

    def ingest_metadata(self, metadata: DataProductMetaData) -> None:
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

            self.dict_of_data_products_metadata[
                data_product_metadata_instance.metadata_dict["execution_block"]
            ] = data_product_metadata_instance
            self.number_of_dataproducts = self.number_of_dataproducts + 1
        except Exception as error:
            logger.error("Failed to ingest ingest_metadata, error: %s", error)
            raise

    def get_metadata(self, execution_block: str) -> dict[str, Any]:
        """Retrieves metadata for the given execution block.

        Args:
            execution_block: The execution block identifier.

        Returns:
            A dictionary containing the metadata for the execution block, or None if not found.
        """
        try:
            return self.dict_of_data_products_metadata[execution_block].metadata_dict
        except KeyError:
            logger.warning("Metadata not found for execution block: %s", execution_block)
            return {}

    def get_data_product_file_path(self, execution_block: str) -> pathlib.Path:
        """Retrieves the file path to the data product for the given execution block.

        Args:
            execution_block: The execution block to retrieve metadata for.

        Returns:
            The file path as a pathlib.Path object, or {} if not found.
        """
        try:
            return pathlib.Path(
                self.dict_of_data_products_metadata[execution_block].metadata_dict[
                    "dataproduct_file"
                ]
            )
        except KeyError:
            logger.warning("File path not found for execution block: %s", execution_block)
            return {}

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
