"""Module to insert data into Elasticsearch instance."""
import datetime
import json
import logging
import pathlib
import subprocess
from typing import Optional

# pylint: disable=no-name-in-module
import pydantic
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ska_sdp_dataproduct_api.core.settings import (
    PERSISTENT_STORAGE_PATH,
    STREAM_CHUNK_SIZE,
    VERSION,
)

# get reference to the logging object
logger = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods


class DPDAPIStatus:
    """This class contains the status and methods related to the Data Product
    dashboard's API"""

    api_running: bool = True
    search_enabled: bool = False
    indexing: bool = False
    date_modified: datetime.datetime = datetime.datetime.now()
    version: str = VERSION

    def status(self, es_search_enabled: bool):
        """Returns the status of the Data Product API"""
        self.search_enabled = es_search_enabled
        return {
            "API_running": True,
            "Indexing": self.indexing,
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
    fullPathName: Optional[pathlib.Path] = None
    metaDataFile: Optional[pathlib.Path] = None

    class Config:
        """Config the behaviour of pydantic"""

        arbitrary_types_allowed = True
        validate_assignment = True
        validate_default = True
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
        path = PERSISTENT_STORAGE_PATH.joinpath(relative_path)
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
            derived_full_path_name = PERSISTENT_STORAGE_PATH.joinpath(values["relativePathName"])
            verify_file_path(derived_full_path_name)
        else:
            verify_file_path(full_path_name)
        return derived_full_path_name or full_path_name


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_value_pairs: list[str] = None


class DataProductMetaData(BaseModel):
    """
    Class containing all information from a MetaData object
    """

    interface: str
    date_created: Optional[str]
    execution_block: str
    metadata_file: Optional[pathlib.Path]
    context: dict
    config: dict
    files: list
    obscore: dict | None = None


def generate_data_stream(file_path: pathlib.Path):
    """This function creates a subprocess that stream a specified file in
    chunks"""
    # create a subprocess to run the tar command
    with subprocess.Popen(
        ["tar", "-C", str(file_path.parent), "-c", str(file_path.name)],
        stdout=subprocess.PIPE,
    ) as process:
        # stream the data from the process output
        chunk = process.stdout.read(STREAM_CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = process.stdout.read(STREAM_CHUNK_SIZE)


def download_file(file_object: FileUrl):
    """This function returns a response that can be used to download a file
    pointed to by the file_object"""
    return StreamingResponse(
        generate_data_stream(file_object.fullPathName),
        media_type="application/x-tar",
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


def get_relative_path(absolute_path: pathlib.Path) -> pathlib.Path:
    """
    Converts an absolute path to a relative path based on a predefined persistent storage path.

    Args:
        absolute_path (pathlib.Path): The absolute path to be converted.

    Returns:
        pathlib.Path: The corresponding relative path. If the `absolute_path` does not start with
        the `PERSISTENT_STORAGE_PATH`, the original `absolute_path` is returned unchanged.
    """
    persistent_storage_path_len = len(PERSISTENT_STORAGE_PATH.parts)
    if absolute_path.parts[:persistent_storage_path_len] == PERSISTENT_STORAGE_PATH.parts:
        return pathlib.Path(*absolute_path.parts[persistent_storage_path_len:])
    return absolute_path


def get_date_from_name(execution_block: str) -> str:
    """
    Extracts a date string from an execution block (type-generatorID-datetime-localSeq from
    https://confluence.skatelescope.org/display/SWSI/SKA+Unique+Identifiers) and converts it
    to the format 'YYYY-MM-DD'.

    Args:
        execution_block (str): A string containing metadata information.

    Returns:
        str: The formatted date string in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If the date cannot be parsed from the execution block.

    Example:
        >>> get_date_from_name("type-generatorID-20230411-localSeq")
        '2023-04-11'
    """
    metadata_date_str = execution_block.split("-")[2]
    year = metadata_date_str[0:4]
    month = metadata_date_str[4:6]
    day = metadata_date_str[6:8]
    try:
        date_obj = datetime.datetime(int(year), int(month), int(day))
        return date_obj.strftime("%Y-%m-%d")
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


def check_date_format(date, date_format):
    """Given a date, check that it is in the expected YYYY-MM-DD format and return a
    datatime object"""
    try:
        return datetime.datetime.strptime(date, date_format)
    except ValueError:
        return logger.error(json.dumps({"Error": "Invalid date format, expected YYYY-MM-DD"}))
