"""
Module for in-memory volume index metadata store.

This module implements the `InMemoryVolumeIndexMetadataStore` class. It provides functionalities
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
from datetime import datetime, timezone
from typing import Any

from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.pv_interface.pv_interface import PVIndex
from ska_dataproduct_api.utilities.helperfunctions import DataProductIdentifier

logger = logging.getLogger(__name__)


class InMemoryVolumeIndexMetadataStore:
    """Class to handle data ingest from various sources"""

    def __init__(self):
        self.number_of_dataproducts: int = 0
        self.dict_of_data_products_metadata: dict[DataProductMetadata] = {}
        self.date_modified = datetime.now(tz=timezone.utc)

    def status(self) -> dict:
        """
        Retrieves the current status of the PostgreSQL connection.

        Returns:
            A dictionary containing the current status information.
        """
        return {
            "store_type": "In memory volume index metadata store",
            "number_of_dataproducts_loaded": self.number_of_dataproducts,
            "last_metadata_update_time": self.date_modified,
        }

    def reload_all_data_products_in_index(self, pv_index: PVIndex) -> None:
        """This method resets and recreates the flattened_list_of_dataproducts_metadata. This is
        added to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        try:
            logger.info("Reloading all data products from PV index into metadata store...")
            self.ingest_list_of_data_product_paths(pv_index=pv_index)
            self.date_modified = datetime.now(tz=timezone.utc)
            logger.info("Reloading into metadata store completed.")
        except Exception as exception:
            raise exception

    def ingest_list_of_data_product_paths(self, pv_index: PVIndex) -> None:
        """
        This method ingests metadata files from a specified storage location into the metadata
        store.

        Args:
            full_path_name: The Path object representing the storage location containing the
            metadata files.

        Returns:
            None
        """
        for _, pv_data_product in pv_index.dict_of_data_products_on_pv.items():
            try:
                _ = self.ingest_file(pv_data_product.path)
            except Exception as error:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to ingest data product at file location: %s, due to error: %s",
                    str(pv_data_product.path),
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
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata(
                data_source="dpd"
            )
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
            str(data_product_metadata_instance.data_product_uid)
        ] = data_product_metadata_instance

        self.number_of_dataproducts = len(self.dict_of_data_products_metadata)

        return data_product_metadata_instance.data_product_uid

    def ingest_metadata(self, metadata_file_dict: dict, data_source: str = "dpd") -> uuid.UUID:
        """
        Ingests a data product,structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_metadata_file_path (pathlib.Path): The path to the data file.

        Returns:
            None
        """
        try:
            data_product_metadata_instance: DataProductMetadata = DataProductMetadata(
                data_source=data_source
            )
            data_product_metadata_instance.load_metadata_from_class(metadata=metadata_file_dict)

        except Exception as error:
            logger.error(
                "Failed to ingest dataproduct metadata: %s. Error: %s",
                metadata_file_dict,
                error,
            )
            raise error

        self.dict_of_data_products_metadata[
            str(data_product_metadata_instance.data_product_uid)
        ] = data_product_metadata_instance

        self.number_of_dataproducts = len(self.dict_of_data_products_metadata)

        return data_product_metadata_instance.data_product_uid

    def get_metadata(self, data_product_itentifier: DataProductIdentifier) -> dict[str, Any]:
        """Retrieves metadata for the given uid.

        Args:
            data_product_uid: The data product uid identifier.

        Returns:
            A dictionary containing the metadata for the uid, or None if not found.
        """
        if not data_product_itentifier.uid:
            logger.warning("Metadata not found for uid: %s", data_product_itentifier.uid)
            return {}

        try:
            return self.dict_of_data_products_metadata[data_product_itentifier.uid].metadata_dict
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Failed to get metadata for execution block, error: %s", error)
            return {}

    def get_data_product_file_paths(
        self, data_product_identifier: DataProductIdentifier
    ) -> list[pathlib.Path]:
        """Retrieves the file paths to the data product(s) for the given identifier.

        Args:
            data_product_identifier: The identifier containing either a UID or ExecutionBlock.

        Returns:
            A list of pathlib.Path objects representing the file paths.

        Raises:
            ValueError: If neither UID nor ExecutionBlock is provided.
            FileNotFoundError: If no data product is found with the given identifier.
        """
        try:
            if data_product_identifier.uid:
                return [
                    pathlib.Path(
                        self.dict_of_data_products_metadata[
                            data_product_identifier.uid
                        ].data_product_file_path
                    )
                ]
            if data_product_identifier.execution_block:
                file_paths: list[pathlib.Path] = []
                for _, metadata in self.dict_of_data_products_metadata.items():
                    if metadata.execution_block == data_product_identifier.execution_block:
                        file_paths.append(pathlib.Path(metadata.data_product_file_path))
                return file_paths

            return []
        except KeyError as error:
            logger.warning(
                "File path not found for data product with identifier: %s",
                data_product_identifier.uid or data_product_identifier.execution_block,
            )
            raise FileNotFoundError(
                f"No data product file path found with data_product_identifier: \
{data_product_identifier} due to error: {error}"
            ) from error
