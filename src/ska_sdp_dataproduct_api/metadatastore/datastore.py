"""Module to capture commonality between in memory and elasticsearch stores."""
import json
import logging
import pathlib
from time import time

import yaml
from ska_sdp_dataproduct_metadata import MetaData

from ska_sdp_dataproduct_api.core.helperfunctions import (
    FileUrl,
    find_metadata,
    get_date_from_name,
    get_relative_path,
)
from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
)

logger = logging.getLogger(__name__)


class Store:
    """Common store class (superclass to elastic search and in memory store)"""

    def __init__(self):
        self.indexing_timestamp = 0
        self.metadata_list = []

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

    def reindex(self):
        """This method resets and recreates the metadata_list. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        self.clear_metadata_indecise()
        self.ingest_metadata_files(PERSISTANT_STORAGE_PATH)
        self.indexing_timestamp = time()
        logger.info("Metadata store cleared and re-indexed")

    def ingest_file(self, path: pathlib.Path):
        """This function gets the file information of a data product and
        structure the information to be inserted into the metadata store.
        """
        metadata_file = path
        metadata_file_name = FileUrl
        metadata_file_name.fullPathName = PERSISTANT_STORAGE_PATH.joinpath(
            get_relative_path(metadata_file)
        )
        metadata_file_name.relativePathName = get_relative_path(metadata_file)
        metadata_file_json = self.load_metadata_file(
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
                data_product_details[query_metadata["key"]] = query_metadata[
                    "value"
                ]
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

        # Iterates through all the items in the metadata_list to see if an
        # entry exist, if it is found, it is replaced, else added to the end.
        for i, product in enumerate(self.metadata_list):
            if (
                product["execution_block"]
                == data_product_details["execution_block"]
            ):
                data_product_details["id"] = product["id"]
                self.metadata_list[i] = data_product_details
                return
        data_product_details["id"] = len(self.metadata_list) + 1
        self.metadata_list.append(data_product_details)
        return

    @staticmethod
    def find_folders_with_metadata_files():
        """This function lists all folders containing a metadata file"""
        folders = []
        for file_path in PERSISTANT_STORAGE_PATH.rglob(METADATA_FILE_NAME):
            if file_path not in folders:
                folders.append(file_path)
        return folders

    @staticmethod
    def load_metadata_file(file_object: FileUrl):
        """This function loads the content of a yaml file and return it as
        json."""
        if not file_object.fullPathName.is_file():
            logger.warning(
                "Metadata file path '%s' not pointing to a file.",
                str(file_object.fullPathName),
            )
            return {}
        try:
            with open(
                file_object.fullPathName, "r", encoding="utf-8"
            ) as metadata_yaml_file:
                metadata_yaml_object = yaml.safe_load(
                    metadata_yaml_file
                )  # yaml_object will be a list or a dict
        except Exception as error:  # pylint: disable=W0718
            # pylint: disable=W0511
            # TODO: The exception is too broad we should strive
            # TODO: to only handle errors we think are reasonable.
            # pylint: enable=W0511
            # Expecting that there will be some errors on ingest of metadata
            # and don't want to break the application when it occurs.
            # Therefore, logging the error to log and returning {}
            logger.warning(
                "Load of metadata file failed for: %s, %s",
                str(file_object.fullPathName),
                error,
            )
            return {}

        # validate the metadata against the schema
        validation_errors = MetaData.validator.iter_errors(
            metadata_yaml_object
        )
        # Loop over the errors
        for validation_error in validation_errors:
            logger.debug(
                "Dataproduct schema validation error when ingesting: %s : %s",
                str(file_object.fullPathName),
                str(validation_error.message),
            )

            if (
                str(validation_error.validator) == "required"
                or str(validation_error.message)
                == "None is not of type 'object'"
            ):
                logger.warning(
                    "Not loading dataproduct due to schema validation error \
    when ingesting: %s : %s",
                    str(file_object.fullPathName),
                    str(validation_error.message),
                )
                return {}

        metadata_date = get_date_from_name(
            metadata_yaml_object["execution_block"]
        )
        metadata_yaml_object.update({"date_created": metadata_date})
        metadata_yaml_object.update(
            {"dataproduct_file": str(file_object.relativePathName.parent)}
        )
        metadata_yaml_object.update(
            {"metadata_file": str(file_object.relativePathName)}
        )
        metadata_json = json.dumps(metadata_yaml_object)
        return metadata_json
