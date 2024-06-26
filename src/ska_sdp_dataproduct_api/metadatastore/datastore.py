"""Module to capture commonality between in memory and elasticsearch stores."""
import json
import logging
import pathlib
from pathlib import Path
from time import time
from typing import Any

import yaml
from ska_sdp_dataproduct_metadata import MetaData

from ska_sdp_dataproduct_api.core.helperfunctions import (
    DataProductMetaData,
    DPDAPIStatus,
    FileUrl,
    find_metadata,
    get_date_from_name,
    get_relative_path,
)
from ska_sdp_dataproduct_api.core.settings import METADATA_FILE_NAME, PERSISTENT_STORAGE_PATH

logger = logging.getLogger(__name__)


class Store:
    """Common store class (superclass to elastic search and in memory store)"""

    def __init__(self, dpd_api_status):
        self.indexing_timestamp = 0
        self.metadata_list = []
        self.dpd_api_status: DPDAPIStatus = dpd_api_status

    @property
    def es_search_enabled(self):
        """This property is implemented in the subclasses."""
        raise NotImplementedError

    def clear_metadata_indecise(self):
        """This method is implemented in the subclasses."""
        raise NotImplementedError

    def insert_metadata(self, metadata_file_json):
        """This is implemented in subclasses."""
        raise NotImplementedError

    def reindex(self) -> None:
        """This method resets and recreates the metadata_list. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        try:
            self.clear_metadata_indecise()
            self.dpd_api_status.indexing = True
            self.ingest_metadata_files(PERSISTENT_STORAGE_PATH)
            self.indexing_timestamp = time()
            self.dpd_api_status.update_data_store_date_modified()
            self.dpd_api_status.indexing = False
            logger.info("Metadata store cleared and re-indexed")
        except Exception as exception:
            self.dpd_api_status.indexing = False
            raise exception

    def sort_metadata_list(self, key: str, reverse: bool) -> None:
        """This method sorts the metadata_list according to the set key"""
        self.metadata_list.sort(key=lambda x: x[key])
        if reverse:
            self.metadata_list.reverse()

    def ingest_file(self, path: pathlib.Path):
        """This function gets the file information of a data product and
        structure the information to be inserted into the metadata store.
        """
        metadata_file = path
        metadata_file_name = FileUrl
        metadata_file_name.fullPathName = PERSISTENT_STORAGE_PATH.joinpath(
            get_relative_path(metadata_file)
        )
        metadata_file_name.relativePathName = get_relative_path(metadata_file)
        metadata_file_json = self.load_metadata(
            metadata_file_name,
        )
        # return if no metadata was read
        if len(metadata_file_json) == 0:
            return
        self.insert_metadata(metadata_file_json)

    def ingest_metadata_files(self, full_path_name: pathlib.Path):
        """This function runs through a volume and add all the data products to
        the metadata_list of the store"""
        # Test if the path points to a directory
        logger.info(
            "Loading metadata files from storage location %s, \
            then ingesting them into the metadata store",
            str(full_path_name),
        )
        if not full_path_name.is_dir() or full_path_name.is_symlink():
            return
        dataproduct_paths = self.find_folders_with_metadata_files()
        for product_path in dataproduct_paths:
            self.ingest_file(product_path)
        self.sort_metadata_list(key="date_created", reverse=True)

    def ingest_metadata_object(self, metadata: DataProductMetaData):
        """
        Ingest a single metadata object
        """
        self.insert_metadata(metadata.json())

        return metadata.dict()

    def add_dataproduct(self, metadata_file, query_key_list):
        """Populate a list of data products and its metadata"""
        data_product_details = {}
        for key, value in metadata_file.items():
            if key in (
                "execution_block",
                "date_created",
                "dataproduct_file",
                "metadata_file",
            ):
                data_product_details[key] = value

        # add additional keys based on the query
        # NOTE: at present users can only query using a single metadata_key,
        #       but add_dataproduct supports multiple query keys
        for query_key in query_key_list:
            query_metadata = find_metadata(metadata_file, query_key)
            if query_metadata is not None:
                data_product_details[query_metadata["key"]] = query_metadata["value"]
        self.update_dataproduct_list(data_product_details)

    def update_dataproduct_list(self, data_product_details):
        """This function looks if the new data product is in the metadata list,
        if it is, the dataproduct entry is replaced, if it is new, it is
        appended
        """
        # Adds the first dictionary to the list
        if len(self.metadata_list) == 0:
            data_product_details["id"] = 1
            self.metadata_list.append(data_product_details)
            return

        data_product_details["id"] = len(self.metadata_list) + 1
        self.metadata_list.append(data_product_details)
        return

    @staticmethod
    def find_folders_with_metadata_files():
        """This function lists all folders containing a metadata file"""
        folders = []
        for file_path in PERSISTENT_STORAGE_PATH.rglob(METADATA_FILE_NAME):
            if file_path not in folders:
                folders.append(file_path)
        return folders

    def check_file_exists(self, file_object: Path) -> bool:
        """
        Checks if the given file path points to an existing file.

        Args:
            file_object (Path): The full path to the file.

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

    def load_metadata_file(self, file_object: FileUrl) -> dict[str, Any]:
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
        except Exception as other_error:
            logger.warning("Unexpected error occurred: %s", other_error)
            raise other_error

    def load_metadata(self, file_object: FileUrl):
        """This function loads the content of a yaml file and return it as
        json."""
        # Test that the metadata file exists
        if not self.check_file_exists(file_object.fullPathName):
            return {}

        # Load the metadata file into memory
        try:
            metadata_yaml_object = self.load_metadata_file(file_object)
        except Exception as error:  # pylint: disable=W0718
            logger.error(
                "Not loading dataproduct due to a loading of metadata failure: %s, %s",
                str(file_object.fullPathName),
                error,
            )
            return {}

        # Validate the metadata against the schema
        validation_errors = MetaData.validator.iter_errors(metadata_yaml_object)

        # Loop over the errors
        for validation_error in validation_errors:
            logger.error(
                "Not loading dataproduct due to schema validation error \
when ingesting: %s : %s",
                str(file_object.fullPathName),
                str(validation_error.message),
            )

            if (
                str(validation_error.validator) == "required"
                or str(validation_error.message) == "None is not of type 'object'"
            ):
                logger.error(
                    "Not loading dataproduct due to schema validation error \
when ingesting: %s : %s",
                    str(file_object.fullPathName),
                    str(validation_error.message),
                )
                return {}

        try:
            metadata_date = get_date_from_name(metadata_yaml_object["execution_block"])
        except Exception as error:  # pylint: disable=W0718
            logger.error(
                "Not loading dataproduct due to failure to extract the date from execution block\
: %s : %s",
                str(file_object.fullPathName),
                error,
            )
            return {}

        metadata_yaml_object.update({"date_created": metadata_date})
        metadata_yaml_object.update({"dataproduct_file": str(file_object.relativePathName.parent)})
        metadata_yaml_object.update({"metadata_file": str(file_object.relativePathName)})
        metadata_json = json.dumps(metadata_yaml_object)
        return metadata_json
