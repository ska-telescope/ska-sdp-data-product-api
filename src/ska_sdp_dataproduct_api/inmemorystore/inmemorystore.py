"""Module to insert data into Elasticsearch instance."""
import json
import logging
from collections.abc import MutableMapping

from ska_sdp_dataproduct_api.core.helperfunctions import (
    add_dataproduct,
    ingest_metadata_files,
)
from ska_sdp_dataproduct_api.core.settings import PERSISTANT_STORAGE_PATH

logger = logging.getLogger(__name__)

# pylint: disable=no-name-in-module


class InMemoryDataproductIndex:
    """
    This class defines an object that is used to create a list of data products
    based on information contained in the metadata files of these data
    products.
    """

    def __init__(self, es_search_enabled) -> None:
        self.metadata_list = []
        if not es_search_enabled:
            ingest_metadata_files(self, PERSISTANT_STORAGE_PATH)

    def reindex(self):
        """This methods resets and recreates the metadata_list. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        self.metadata_list.clear()
        ingest_metadata_files(self, PERSISTANT_STORAGE_PATH)
        logger.info("Metadata store cleared and re-indexed")

    def insert_metadata(self, metadata_file_json):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the metadata_list"""
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = self.generate_metadata_keys_list(
            metadata_file, ["files"], "", "."
        )

        add_dataproduct(
            self.metadata_list,
            metadata_file=metadata_file,
            query_key_list=query_key_list,
        )

    def generate_metadata_keys_list(
        self, metadata, ignore_keys, parent_key="", sep="_"
    ):
        """Given a nested dict, return the flattened list of keys"""
        items = []
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(
                    self.generate_metadata_keys_list(
                        value, ignore_keys, new_key, sep=sep
                    )
                )
            else:
                if new_key not in ignore_keys:
                    items.append(new_key)
        return items
