"""Module to insert data into Elasticsearch instance."""
import datetime
import json
import os
import pathlib
from pathlib import Path

import yaml
from fastapi import HTTPException

# pylint: disable=no-name-in-module
from pydantic import BaseModel

from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
)

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str = "Untitled"
    fileName: str


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_pair: str = ""


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


def find_metadata(metadata, query_key):
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


def update_dataproduct_list(metadata_list, metadata_file: str, query_key_list):
    """Populate a list of data products and its metadata"""
    data_product_details = {}
    data_product_details["id"] = len(metadata_list) + 1
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
        query_metadata = find_metadata(metadata_file, query_key)
        if query_metadata is not None:
            data_product_details[query_metadata["key"]] = query_metadata[
                "value"
            ]

    metadata_list.append(data_product_details)
