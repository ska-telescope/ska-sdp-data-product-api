"""Module to insert data into Elasticsearch instance."""
import datetime
import json
import logging
import pathlib
import subprocess
from typing import Any, Dict, List, Optional

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


def filter_integers(item: int, operator: str, value: int) -> bool:
    """
    Filters a list of values based on a single field, operator, and integer value.

    Args:
        item_values: The list of values to filter.
        operator: The filtering operation to perform.
        value: The integer value to compare with.

    Returns:
        A list of indexes of matching items in the original data list.
    """

    match operator:
        case "equals":
            if item == value:
                return True
        case "isAnyOf":
            if str(value) in str(item).split(","):
                return True
        case _:
            raise ValueError(f"Unsupported filter operator for integers: {operator}")
    return False


def filter_strings(item: str, operator: str, value: str) -> bool:
    """
    Filters a list of values based on a single field, operator, and string value.

    Args:
        item_values: The list of values to filter.
        operator: The filtering operation to perform.
        value: The string value to compare with.

    Returns:
        A list of indexes of matching items in the original data list.
    """
    if item is None:
        item = ""  # Handle None values as empty strings
    match operator:
        case "contains":
            if value in str(item):
                return True
        case "equals":
            if item == value:
                return True
        case "startsWith":
            if str(item).startswith(value):
                return True
        case "endsWith":
            if str(item).endswith(value):
                return True
        case "isEmpty":
            if not item:
                return True
        case "isNotEmpty":
            if item:
                return True
        case "isAnyOf":
            if item in value.split(","):
                return True
        case _:
            raise ValueError(f"Unsupported filter operator for strings: {operator}")
    return False


def filter_datetimes(
    item: datetime.datetime, operator: str, value: datetime.datetime
) -> List[Dict[str, Any]]:
    """
    Filters a list of values based on a single field, operator, and datetime value.

    Args:
        item_values: The list of values to filter.
        operator: The filtering operation to perform (supports "equals" and "isAnyOf").
        value: The datetime value to compare with.

    Returns:
        A list of indexes of matching items in the original data list.
    """

    if item is None:
        return False  # Skip None values
    match operator:
        case "equals":
            if item == value:
                return True
        case "greaterThan":
            if item >= value:
                return True
        case "lessThan":
            if item <= value:
                return True
        case "isAnyOf":
            try:
                # Attempt to convert string representation of datetime to datetime object
                date_values = [
                    datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S") for v in value.split(",")
                ]
                if item in date_values:
                    return True
            except ValueError as exception:
                raise exception  # Re-raise the ValueError for the caller to handle
        case _:
            raise ValueError(f"Unsupported filter operator for datetimes: {operator}")
    return False


def filter_by_item(
    data: List[Dict[str, Any]], field: str, operator: str, value: Any
) -> List[Dict[str, Any]]:
    """
    Filters a list of dictionaries based on a single field, operator, and value.

    Args:
        data: The list of dictionaries to filter.
        field: The field name to filter on.
        operator: The filtering operation to perform (e.g., "contains", "equals", "startsWith",
        "endsWith", "isEmpty", "isNotEmpty", "isAnyOf").
        value: The value to compare with the field.

    Raises:
        ValueError: If an unsupported filter operator is provided.

    Returns:
        A new list containing only the dictionaries that match the filter criteria.
    """

    filtered_data: List[Dict[str, Any]] = []

    for item in data:
        try:
            item_value = item.get(field)

            # Delegate filtering based on value type (integer or string)
            if isinstance(value, int):
                if filter_integers(item_value, operator, value):
                    filtered_data.append(item)
            elif isinstance(value, datetime.datetime):
                date_value = parse_valid_date(item_value, "%Y-%m-%d")
                if filter_datetimes(date_value, operator, value):
                    filtered_data.append(item)
            else:
                if filter_strings(item_value, operator, value):
                    filtered_data.append(item)
        except ValueError:
            logging.error("Failed to filter on item %s", str(item))

    return filtered_data


def has_nested_status(item: dict | list, searched_key: str, searched_value: str) -> bool:
    """
    Searches for a nested key-value pair within a dictionary or list structure.

    Args:
        item (dict | list): The dictionary or list to search within.
        searched_key (str): The key to search for within nested dictionaries.
        searched_value (str): The value to search for within the nested dictionary
                              associated with the searched_key.

    Returns:
        bool: True if the key-value pair is found nested within the item, False otherwise.

    Raises:
        TypeError: If the `item` is not a dictionary or list.
    """

    if not isinstance(item, (dict, list)):
        raise TypeError(f"Expected item to be a dictionary or list, got {type(item)}")

    for key, value in item.items() if isinstance(item, dict) else enumerate(item):
        if key and value:
            if searched_key in str(key) and searched_value in str(value):
                return True

            if isinstance(value, (dict, list)):
                if has_nested_status(value, searched_key, searched_value):
                    return True

    return False


def filter_by_key_value_pair(
    data: List[Dict[str, Any]], key_value_pairs: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Filters a list of dictionaries based on key-value pairs.

    Args:
        data: A list of dictionaries where each dictionary represents a data point.
        key_value_pairs: A list of dictionaries where each dictionary contains a "keyPair" key and 
        a "valuePair" key.
            The function filters the "data" list based on these key-value pairs.

    Returns:
        A new list of dictionaries containing elements from "data" that match all key-value pairs 
        in "key_value_pairs".
    """
    filtered_data = data.copy()  # Avoid modifying the original data

    for key_value_pair in key_value_pairs:
        searched_key = key_value_pair.get("keyPair")
        searched_value = key_value_pair.get("valuePair")

        filtered_data = [
            item for item in filtered_data if has_nested_status(item, searched_key, searched_value)
        ]

    return filtered_data


def parse_valid_date(date_string: str, expected_format: str) -> datetime.datetime:
    """Parses a date string into a datetime object if the format is valid.

    Args:
        date_string: The date string to parse (e.g., "2023-07-01").
        expected_format: The expected format of the date string (e.g., "%Y-%m-%d").

    Returns:
        A datetime object if the date is valid in the specified format, otherwise raises a
        ValueError.

    Raises:
        ValueError: If the date format is invalid.
    """

    try:
        return datetime.datetime.strptime(date_string, expected_format)
    except ValueError as exception:
        logging.error("Invalid date format: %s. Expected format: %s", date_string, expected_format)
        raise exception  # Re-raise the ValueError for the caller to handle
