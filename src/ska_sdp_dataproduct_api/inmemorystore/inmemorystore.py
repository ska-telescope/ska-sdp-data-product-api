"""Module to insert data into Elasticsearch instance."""
import json
import os
from collections.abc import MutableMapping
from pathlib import Path

from ska_sdp_dataproduct_api.core.helperfunctions import (
    FileUrl,
    loadmetadatafile,
    relativepath,
    update_dataproduct_list,
    verify_file_path,
)
from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
)

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
            self.ingestmetadatafiles(PERSISTANT_STORAGE_PATH)
            print("ingesting data")

    def ingestmetadatafiles(self, storage_path: str):
        """This method runs through a volume and add all the data products to
        the elk_metadata_store or into the metadata_list if the store is not
        available"""
        if verify_file_path(storage_path):
            # Test if the path points to a directory
            if os.path.isdir(storage_path) and not os.path.islink(
                storage_path
            ):
                # For each file in the directory,
                files = os.listdir(storage_path)
                # test if the directory contains a metadatafile
                if METADATA_FILE_NAME in files:
                    # If it contains the metadata file add it to the index
                    dataproduct_file_name = relativepath(storage_path)
                    metadata_file = Path(storage_path).joinpath(
                        METADATA_FILE_NAME
                    )
                    metadata_file_name = FileUrl
                    metadata_file_name.relativeFileName = relativepath(
                        metadata_file
                    )
                    metadata_file_json = loadmetadatafile(
                        metadata_file_name,
                        dataproduct_file_name,
                        metadata_file_name.relativeFileName,
                    )
                    self.insert_metadata(metadata_file_json)
                else:
                    # If it is not a data product, enter the folder and repeat
                    # this test.
                    for file in os.listdir(storage_path):
                        self.ingestmetadatafiles(
                            os.path.join(storage_path, file)
                        )
            return ""
        return "Metadata ingested"

    def reindex(self):
        """This methods resets and recreates the metadata_list. This is added
        to enable the user to reindex if the data products were changed or
        appended since the initial load of the data"""
        self.metadata_list.clear()
        print("Reinexing")
        self.ingestmetadatafiles(PERSISTANT_STORAGE_PATH)

    def insert_metadata(self, metadata_file_json):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the metadata_list"""
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = self.generatemetadatakeyslist(
            metadata_file, ["files"], "", "."
        )

        update_dataproduct_list(
            self.metadata_list,
            metadata_file=metadata_file,
            query_key_list=query_key_list,
        )

    def generatemetadatakeyslist(
        self, metadata, ignore_keys, parent_key="", sep="_"
    ):
        """Given a nested dict, return the flattened list of keys"""
        items = []
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(
                    self.generatemetadatakeyslist(
                        value, ignore_keys, new_key, sep=sep
                    )
                )
            else:
                if new_key not in ignore_keys:
                    items.append(new_key)
        return items
