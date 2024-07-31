import datetime
import logging
import pathlib
from time import time
from typing import Any, List

import yaml
from ska_sdp_dataproduct_metadata import MetaData

from ska_sdp_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_sdp_dataproduct_api.configuration.settings import (
    METADATA_FILE_NAME,
    PERSISTENT_STORAGE_PATH,
)
from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    DataProductMetaData,
    FilePaths,
    find_metadata,
    get_date_from_name,
    get_relative_path,
)

logger = logging.getLogger(__name__)


class in_memory_volume_index_metadata_store:
    """Class to handle data ingest from various sources"""

    def __init__(self):
        self.postgresql_running: bool = False
        self.number_of_dataproducts: int = 0
        self.list_of_data_product_paths: List[pathlib.Path] = []
        self.list_of_data_products_metadata: list[dict] = []
        self.load_data_products()

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

    def load_data_products(self):
        """ """
        self.load_data_products_from_persistent_volume()

    def load_data_products_from_persistent_volume(self) -> None:
        """ """
        self.reindex_persistent_volume()  # TODO This need to change

    def reindex(self) -> None:
        """This method resets and recreates the flattened_list_of_dataproducts_metadata. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        self.reindex_persistent_volume()

    def reindex_persistent_volume(self) -> None:
        """This method resets and recreates the flattened_list_of_dataproducts_metadata. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        try:
            logger.info("Re-indexing persistent volume store...")
            self.indexing = True
            self.clear_metadata_indecise()
            self.list_all_data_product_files(PERSISTENT_STORAGE_PATH)
            self.ingest_list_of_data_product_paths()
            self.indexing_timestamp = time()
            self.update_data_store_date_modified()
            self.indexing = False
            logger.info("Metadata store cleared and re-indexed")
        except Exception as exception:
            self.indexing = False
            raise exception

    def clear_metadata_indecise(self):
        """Clears metadata information stored within the class instance.

        This method clears the `flattened_list_of_dataproducts_metadata` attribute
        and sets the `number_of_dataproducts` attribute to 0.
        """
        muiDataGridInstance.flattened_list_of_dataproducts_metadata.clear()
        self.number_of_dataproducts = 0

    def list_all_data_product_files(self, full_path_name: pathlib.Path) -> None:
        """
        Lists all data product files within the specified directory path.

        This method recursively traverses the directory structure starting at `full_path_name`
        and identifies files that are considered data products based on pre-defined criteria
        of the folder containing a metadata file.

        Args:
            full_path_name (pathlib.Path): The path to the directory containing data products.

        Returns:
            List[pathlib.Path]: A list of `pathlib.Path` objects representing the identified
                                data product files within the directory and its subdirectories.
                                If no data product files are found, an empty list is returned.

        Raises:
            ValueError: If `full_path_name` does not represent a valid directory or is a symbolic
            link.
        """

        if not full_path_name.is_dir():
            logger.warning("Invalid directory path: %s", full_path_name)

        if full_path_name.is_symlink():
            logger.warning("Symbolic links are not supported:  %s", full_path_name)

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

    def update_data_store_date_modified(self):
        """This method updates the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()

    def ingest_file(self, data_product_path: pathlib.Path) -> None:
        """
        Ingests a data product file by loading its metadata, structuring the information,
        and inserting it into the metadata store.

        Args:
            data_product_path (pathlib.Path): The path to the data file.
        """
        if not data_product_path.is_file():
            logger.error("Invalid path: %s. Expected a file.", data_product_path)

        data_product_file_paths = FilePaths
        data_product_file_paths.fullPathName = PERSISTENT_STORAGE_PATH.joinpath(
            get_relative_path(data_product_path)
        )
        data_product_file_paths.relativePathName = get_relative_path(data_product_path)

        data_product_metadata_dict = self.load_metadata(data_product_file_paths)

        # Check if any metadata was actually loaded before inserting
        if not data_product_metadata_dict:
            return

        # persistent_metadata_store.save_metadata_to_postgresql(metadata_file_json)       # TODO Fix this
        self.list_of_data_products_metadata.append(data_product_metadata_dict)
        self.insert_metadata_in_search_store(data_product_metadata_dict)

    def load_metadata(self, file_object: FilePaths) -> dict[str, Any]:
        """This function loads the content of a yaml file and returns it as a dict."""
        # Test that the metadata file exists
        if not self.check_file_exists(file_object.fullPathName):
            return {}

        # Load the metadata file into memory
        try:
            metadata_dict = self.load_metadata_file(file_object)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Not loading dataproduct due to a loading of metadata failure: %s, %s",
                str(file_object.fullPathName),
                exception,
            )
            return {}

        # Validate the metadata against the schema
        validation_errors = MetaData.validator.iter_errors(metadata_dict)

        # Loop over the errors
        for validation_error in validation_errors:
            logger.error(
                "Not loading dataproduct due to schema validation error when ingesting: %s : %s",
                str(file_object.fullPathName),
                str(validation_error.message),
            )

            if (
                str(validation_error.validator) == "required"
                or str(validation_error.message) == "None is not of type 'object'"
            ):
                logger.error(
                    "Not loading dataproduct due to schema validation error when ingesting: %s : %s",
                    str(file_object.fullPathName),
                    str(validation_error.message),
                )
                return {}

        try:
            metadata_date = get_date_from_name(metadata_dict["execution_block"])
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Not loading dataproduct due to failure to extract the date from execution block: %s : %s",
                str(file_object.fullPathName),
                exception,
            )
            return {}

        metadata_dict.update(
            {
                "date_created": metadata_date,
                "dataproduct_file": str(file_object.relativePathName.parent),
                "metadata_file": str(file_object.relativePathName),
            }
        )

        return metadata_dict

    def insert_metadata_in_search_store(self, data_product_metadata_dict: dict):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the flattened_list_of_dataproducts_metadata"""
        # generate a list of keys from this object
        muiDataGridInstance.update_flattened_list_of_keys(data_product_metadata_dict)
        muiDataGridInstance.update_flattened_list_of_dataproducts_metadata(
            muiDataGridInstance.flatten_dict(data_product_metadata_dict)
        )
        self.number_of_dataproducts = self.number_of_dataproducts + 1

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

    def load_metadata_file(self, file_object: FilePaths) -> dict[str, Any]:
        """
        Load metadata from a YAML file.

        Args:
            file_object: An object representing the file to read metadata from.

        Returns:
            A dictionary containing the loaded metadata, or an empty dictionary if an error occurs
            during loading.
        """
        try:
            with open(file_object.fullPathName, "r", encoding="utf-8") as metadata_yaml_file:
                metadata_yaml_object = yaml.safe_load(metadata_yaml_file)
                return metadata_yaml_object
        except FileNotFoundError as file_not_found_error:
            logger.warning("Metadata file not found: %s", str(file_object.fullPathName))
            raise file_not_found_error
        except yaml.YAMLError as yaml_error:
            logger.warning("Error while parsing YAML: %s", yaml_error)
            raise yaml_error
        except Exception as exception:
            logger.warning("Unexpected error occurred: %s", exception)
            raise exception
