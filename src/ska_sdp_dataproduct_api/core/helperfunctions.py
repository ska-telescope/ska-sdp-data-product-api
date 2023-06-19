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
    ES_HOST,
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
    app,
)

class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str = "Untitled"
    fileName: str


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_pair: str = ""


class TreeIndex:
    """This class contains tree_item_id; an ID field, indicating the next
    tree item id to use, and tree_data dictionary, containing a json object
    that represens a tree structure that can easily be rendered in JS.
    """

    def __init__(self, root_tree_item_id, tree_data):
        self.tree_item_id = root_tree_item_id
        self.tree_data: dict = tree_data
        self.data_product_list: list = []

    def append_children(self, new_data):
        """Merge current dict with new data"""
        self.data_product_list.append(new_data)
        self.tree_data["children"] = self.data_product_list

def verify_file_path(file_path):
    """Test if the file path exists"""
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File path with name {file_path} not found",
        )
    return True

def relativepath(absolute_path):
    """This function returns the relative path of an absolute path where the
    absolute path = PERSISTANT_STORAGE_PATH + relative_path"""
    persistant_storage_path_len = len(Path(PERSISTANT_STORAGE_PATH).parts)
    relative_path = str(
        pathlib.Path(
            *pathlib.Path(absolute_path).parts[(persistant_storage_path_len):]
        )
    )
    return relative_path

def getdatefromname(filename: str):
    """This function extracts the date from the file named according to the
    following format: type-generatorID-datetime-localSeq.
    https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers"""
    metadata_date_str = filename.split("-")[2]
    year = metadata_date_str[0:4]
    month = metadata_date_str[4:6]
    day = metadata_date_str[6:8]
    try:
        datetime.datetime(int(year), int(month), int(day))
        return year + "-" + month + "-" + day
    except ValueError:
        return datetime.date.today().strftime("%Y-%m-%d")

def loadmetadatafile(
    path_to_selected_file: FileUrl,
    dataproduct_file_name="",
    metadata_file_name="",
):
    """This function loads the content of a yaml file and return it as
    json."""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, path_to_selected_file.relativeFileName
    )
    if verify_file_path(
        persistant_file_path
    ) and persistant_file_path.endswith(METADATA_FILE_NAME):
        with open(
            persistant_file_path, "r", encoding="utf-8"
        ) as metadata_yaml_file:
            metadata_yaml_object = yaml.safe_load(
                metadata_yaml_file
            )  # yaml_object will be a list or a dict
        metadata_date = getdatefromname(
            metadata_yaml_object["execution_block"]
        )
        metadata_yaml_object.update({"date_created": metadata_date})
        metadata_yaml_object.update(
            {"dataproduct_file": dataproduct_file_name}
        )
        metadata_yaml_object.update({"metadata_file": metadata_file_name})
        metadata_json = json.dumps(metadata_yaml_object)
        return metadata_json
    return {}

