"""MetadataStore module

This module provides a `MetadataStore` class to manage and update metadata associated with data
products.

The `MetadataStore` class offers functionalities to:

- Store timestamps for indexing and data modification.
- Update the date modified timestamp when data is added or modified.

"""

import datetime
import logging
import pathlib
from time import time

from ska_dataproduct_api.configuration.settings import METADATA_FILE_NAME, PERSISTENT_STORAGE_PATH
from ska_dataproduct_api.utilities.helperfunctions import verify_persistent_storage_file_path

logger = logging.getLogger(__name__)

# pylint: disable=too-few-public-methods


class MetadataStore:
    """
    This class contain methods common to the InMemoryVolumeIndexMetadataStore and the PostgreSQL
    store
    """

    def __init__(self):
        self.indexing_timestamp: time = time()
        self.indexing: bool = False
        self.date_modified = datetime.datetime.now()

    def update_data_store_date_modified(self):
        """This method updates the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()

    def list_all_data_product_files(self, full_path_name: pathlib.Path) -> list:
        """
        Lists all data product files within the specified directory path.

        This method recursively traverses the directory structure starting at `full_path_name`
        and identifies files that are considered data products based on pre-defined criteria
        of the folder containing a metadata file.

        Args:
            full_path_name (pathlib.Path): The path to the directory containing data products.

        Returns:
            list[pathlib.Path]: A list of `pathlib.Path` objects representing the identified
                                data product files within the directory and its subdirectories.
                                If no data product files are found, an empty list is returned.

        Raises:
            ValueError: If `full_path_name` does not represent a valid directory or is a symbolic
            link.
        """

        if not verify_persistent_storage_file_path(full_path_name):
            return []

        logger.info("Identifying data product files within directory: %s", full_path_name)
        list_of_data_product_paths = []
        for file_path in PERSISTENT_STORAGE_PATH.rglob(METADATA_FILE_NAME):
            if file_path not in list_of_data_product_paths:
                list_of_data_product_paths.append(file_path)

        print(str(list_of_data_product_paths))
        return list_of_data_product_paths
