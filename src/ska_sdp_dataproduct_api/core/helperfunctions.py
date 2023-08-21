"""Module to insert data into Elasticsearch instance."""
import datetime
import gzip
import json
import logging
import pathlib
import tarfile
from io import BytesIO
from typing import Optional

import jsonschema

# pylint: disable=no-name-in-module
import pydantic
import requests
import yaml
from fastapi import HTTPException, Response
from pydantic import BaseModel

from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    METADATA_JSON_SCHEMA_FILE,
    PERSISTANT_STORAGE_PATH,
    VERSION,
)

# get reference to the logging object
logger = logging.getLogger(__name__)

# load the metadata schema and create a single validator that can be used
# for every incoming metadata file
with open(
    METADATA_JSON_SCHEMA_FILE, "r", encoding="utf-8"
) as metadata_schema_file:
    metadata_validator = jsonschema.validators.Draft202012Validator(
        json.load(metadata_schema_file)
    )

# pylint: disable=too-few-public-methods


class DPDAPIStatus:
    """This class contains the status and methods related to the Data Product
    dashboard's API"""

    api_running: bool = True
    search_enabled: bool = False
    date_modified: datetime.datetime = datetime.datetime.now()
    version: str = VERSION

    def status(self, es_search_enabled: bool):
        """Returns the status of the Data Product API"""
        self.search_enabled = es_search_enabled
        return {
            "API_running": True,
            "Search_enabled": self.search_enabled,
            "Date_modified": self.date_modified,
            "Version": self.version,
        }

    def update_data_store_date_modified(self):
        """This method update the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()


class FileUrl(BaseModel):
    """
    A class that represents a file URL.

    Attributes:
        fileName (str): The name of the file.
        relativePathName (pathlib.Path): The relative path name of the file.
        fullPathName (pathlib.Path): The full path name of the file.
        metaDataFile (pathlib.Path): The metadata file of the file.

    """

    fileName: str
    relativePathName: pathlib.Path = None
    fullPathName: Optional[pathlib.Path]
    metaDataFile: Optional[pathlib.Path] = None

    class Config:
        """Config the behaviour of pydantic"""

        arbitrary_types_allowed = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"

    @pydantic.validator("relativePathName")
    @classmethod
    def relative_path_name_validator(cls, relative_path: pathlib.Path):
        """
        A validator that validates the relative path name.

        Args:
            relative_path (pathlib.Path): The relative path name.

        Returns:
            pathlib.Path: The validated relative path name.

        Raises:
            HTTPException: If the path is invalid.

        """
        path = PERSISTANT_STORAGE_PATH.joinpath(relative_path)
        verify_file_path(path)
        return relative_path

    @pydantic.validator("fullPathName", pre=True)
    @classmethod
    def full_path_name_validator(cls, full_path_name: pathlib.Path, values):
        """
        A validator that validates the full path name.

        Args:
            full_path_name (pathlib.Path): The full path name.
            values (dict): The values of the attributes.

        Returns:
            pathlib.Path: The validated full path name.

        Raises:
            HTTPException: If the path is invalid.

        """
        if full_path_name is None:
            derived_full_path_name = PERSISTANT_STORAGE_PATH.joinpath(
                values["relativePathName"]
            )
            verify_file_path(derived_full_path_name)
        else:
            verify_file_path(full_path_name)
        return derived_full_path_name or full_path_name


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_pair: str = ""


def gzip_file(file_path: pathlib.Path):
    """Create a gzip response from a file or folder path.

    Args:
        path (Path): The file or folder path to compress.

    Returns:
        requests.Response: A response object with the compressed content.
    """
    # Create a temporary tarfile object
    with tarfile.open(fileobj=BytesIO(), mode="w") as tar:
        # Add the file or folder to the tarfile object
        tar.add(file_path, arcname=file_path.name)
    # Get the content of the tarfile object as bytes
    content = tar.fileobj.getvalue()
    # Compress the content using gzip
    compressed_content = gzip.compress(content)
    # Create a BytesIO object from the compressed content
    compressed_file = BytesIO(compressed_content)
    # Create a new response object with the compressed file
    gzip_response = requests.Response()
    gzip_response.status_code = 200
    gzip_response.headers["Content-Encoding"] = "gzip"
    gzip_response.headers[
        "Content-Disposition"
    ] = f"attachment; filename={file_path.name}.tar.gz"
    gzip_response.raw = compressed_file
    return gzip_response


def download_file(file_object: FileUrl):
    """This function returns a response that can be used to download a file
    pointed to by the file_object"""
    response = gzip_file(file_object.fullPathName)
    return Response(
        content=response.raw.read(),
        status_code=response.status_code,
        headers=response.headers,
    )


def verify_file_path(file_path: pathlib.Path):
    """
    A function that verifies the file path.

    Args:
        file_path (pathlib.Path): The file path.

    Returns:
        bool: True if the file path exists.

    Raises:
        HTTPException: If the file path does not exist.

    """
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"File path with name '{str(file_path)}' not found",
        )
    return True


def get_relative_path(absolute_path):
    """This function returns the relative path of an absolute path where the
    absolute path = PERSISTANT_STORAGE_PATH + relative_path"""
    persistant_storage_path_len = len(PERSISTANT_STORAGE_PATH.parts)
    relative_path = str(
        pathlib.Path(
            *pathlib.Path(absolute_path).parts[(persistant_storage_path_len):]
        )
    )
    return pathlib.Path(relative_path)


def get_date_from_name(execution_block: str):
    """This function extracts the date from the execution_block named according
    to the following format: type-generatorID-datetime-localSeq.
    https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers"""
    metadata_date_str = execution_block.split("-")[2]
    year = metadata_date_str[0:4]
    month = metadata_date_str[4:6]
    day = metadata_date_str[6:8]
    try:
        datetime.datetime(int(year), int(month), int(day))
        return year + "-" + month + "-" + day
    except ValueError as error:
        logger.warning(
            "Date retrieved from execution_block '%s' caused and error: %s",
            execution_block,
            error,
        )
        raise

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

