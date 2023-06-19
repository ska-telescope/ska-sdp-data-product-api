"""Module to insert data into Elasticsearch instance."""
import datetime
import io
import json
import os
import pathlib
import zipfile
from collections.abc import MutableMapping
from pathlib import Path

import yaml
from fastapi import HTTPException, Response

# pylint: disable=no-name-in-module
from pydantic import BaseModel
from starlette.responses import FileResponse

from ska_sdp_dataproduct_api.core.settings import (
    PERSISTANT_STORAGE_PATH,
)

from ska_sdp_dataproduct_api.core.helperfunctions import (
    verify_file_path,
    relativepath,
    FileUrl,
    loadmetadatafile,
)

from ska_sdp_dataproduct_api.core.settings import (
    ES_HOST,
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
    app,
)

class InMemoryDataproductIndex():
    """
        """
    def __init__(self, es_search_enabled) -> None:
        self.metadata_list = []
        if not es_search_enabled:
            self.ingestmetadatafiles(PERSISTANT_STORAGE_PATH)
            print("ingesting data")

    def ingestmetadatafiles(self, storage_path: str):
        """This function runs through a volume and add all the data products to the
        elk_metadata_store or into the metadata_list if the store is not available"""
        if verify_file_path(storage_path):
            # Test if the path points to a directory
            if os.path.isdir(storage_path) and not os.path.islink(storage_path):
                # For each file in the directory,
                files = os.listdir(storage_path)
                # test if the directory contains a metadatafile
                if METADATA_FILE_NAME in files:
                    # If it contains the metadata file add it to the index
                    dataproduct_file_name = relativepath(storage_path)
                    metadata_file = Path(storage_path).joinpath(METADATA_FILE_NAME)
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
                        self.ingestmetadatafiles(os.path.join(storage_path, file))
            return ""
        return "Metadata ingested"

    def reindex(self):
        """
        """
        self.metadata_list.clear
        print("Reinexing")
        self.ingestmetadatafiles(PERSISTANT_STORAGE_PATH)

    def insert_metadata(self,metadata_file_json):
        """
        """
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = self.generatemetadatakeyslist(
            metadata_file, ["files"], "", "."
        )

        self.update_dataproduct_list(
            metadata_file=metadata_file, query_key_list=query_key_list
        )

    def update_dataproduct_list(self, metadata_file: str, query_key_list):
        """Populate a list of data products and its metadata"""
        data_product_details = {}
        data_product_details["id"] = len(self.metadata_list) + 1
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
        #       but update_dataproduct_list supports multiple query keys
        for query_key in query_key_list:
            query_metadata = self.find_metadata(metadata_file, query_key)
            if query_metadata is not None:
                data_product_details[query_metadata["key"]] = query_metadata[
                    "value"
                ]

        self.metadata_list.append(data_product_details)

    def find_metadata(self,metadata, query_key):
        """Given a dict of metadata, and a period-separated hierarchy of keys,
        return the key and the value found within the dict.
        For example: Given a dict and the key a.b.c,
        return the key (a.b.c) and the value dict[a][b][c]"""
        keys = query_key.split(".")

        subsection = metadata
        for key in keys:
            if key in subsection:
                subsection = subsection[key]
            else:
                return None

        return {"key": query_key, "value": subsection}
    
    def generatemetadatakeyslist(self, metadata, ignore_keys, parent_key="", sep="_"):
        """Given a nested dict, return the flattened list of keys"""
        items = []
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(
                    self.generatemetadatakeyslist(value, ignore_keys, new_key, sep=sep)
                )
            else:
                if new_key not in ignore_keys:
                    items.append(new_key)
        return items