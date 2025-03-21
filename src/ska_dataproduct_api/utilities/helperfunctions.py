"""Module contains helper functions used in the project."""
import logging
import os
import pathlib
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any, Generator, Optional, Union

# pylint: disable=no-name-in-module
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ska_dataproduct_api.configuration.settings import (
    PERSISTENT_STORAGE_PATH,
    STREAM_CHUNK_SIZE,
    VERSION,
)

# get reference to the logging object
logger = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods


class DPDAPIStatus:  # pylint: disable=too-many-instance-attributes
    """This class contains the status and methods related to the Data Product
    dashboard's API"""

    def __init__(
        self,
        pv_interface_status: object = None,
        search_store_status: object = None,
        metadata_store_status: object = None,
    ):
        self.api_running = True

        self.version: str = VERSION
        self.pv_interface_status: object = pv_interface_status
        self.metadata_store_status: object = metadata_store_status
        self.search_store_status: object = search_store_status
        self.indexing: bool = False
        self.indexing_timestamp: datetime = datetime.now(tz=timezone.utc)
        self.startup_time: datetime = datetime.now(tz=timezone.utc)
        self.request_count: int = 0  # Added: Request count
        self.error_count: int = 0  # Added: Error count

    def status(self) -> dict:
        """Returns the status of the Data Product API"""
        return {
            "api_running": True,
            "api_version": self.version,
            "startup_time": self.startup_time.isoformat(),
            "indexing": self.indexing,
            "indexing_timestamp": self.indexing_timestamp,
            "self.pv_interface_status": self.pv_interface_status(),
            "metadata_store_status": self.metadata_store_status(),
            "search_store_status": self.search_store_status(),
        }

    def increment_request_count(self):
        """Increments the request count"""
        self.request_count += 1

    def increment_error_count(self):
        """Increments the error count"""
        self.error_count += 1

    def get_error_rate(self) -> float:
        """Calculates and returns the error rate as a percentage"""
        if self.request_count == 0:
            return 0.0
        return (self.error_count / self.request_count) * 100


class FilePaths(BaseModel):
    """
    A class that represents a file URL.

    Attributes:
        execution_block (str): The name of the file.
        relativePathName (pathlib.Path): The relative path name of the file.
        fullPathName (pathlib.Path): The full path name of the file.
        metaDataFile (pathlib.Path): The metadata file of the file.

    """

    execution_block: str
    relativePathName: pathlib.Path = None
    fullPathName: Optional[pathlib.Path] = None
    metaDataFile: Optional[pathlib.Path] = None


class ExecutionBlock(BaseModel):
    """Class for defining search parameters"""

    execution_block: str = None


class DataProductIdentifier(BaseModel):
    """Class for defining Data Product identifiers"""

    uid: Union[str, uuid.UUID] | None = None
    execution_block: str | None = None
    relative_path_name: str | None = None
    meta_data_file: str | None = None
    data_store: str | None = "dpd"


def validate_data_product_identifier(data_product_identifier: DataProductIdentifier) -> None:
    """
    Verify that a DataProductIdentifier has either a UUID or an execution_block.

    Args:
        data_product_identifier: The DataProductIdentifier to validate.

    Returns:
        None

    Raises:
        ValueError: If neither a UUID nor an execution_block is present.
        TypeError: If the input is not a DataProductIdentifier.
    """
    if not isinstance(data_product_identifier, DataProductIdentifier):
        raise TypeError("Input must be of type DataProductIdentifier")

    if not data_product_identifier.uid and not data_product_identifier.execution_block:
        raise ValueError("DataProductIdentifier must have either a UUID or an execution_block.")


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_value_pairs: list[str] = None


class PydanticDataProductMetadataModel(BaseModel):
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


def generate_data_stream(file_path_list: list[pathlib.Path]) -> Generator[bytes, None, None]:
    """
    Generates a stream of data chunks from the specified file path using the `tar` command.

    Args:
        file_path (pathlib.Path): The path to the file to read.

    Yields:
        bytes: Chunks of data read from the file compressed as a tar archive.
    """
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
        temp_file.truncate(0)
        for file_path in file_path_list:
            relative_path = file_path.resolve().relative_to(PERSISTENT_STORAGE_PATH.resolve())
            temp_file.write(str(relative_path) + "\n")

    file_paths_str = temp_file.name

    # create a subprocess to run the tar command

    with subprocess.Popen(
        ["tar", "-C", PERSISTENT_STORAGE_PATH.resolve(), "-c", "-T", file_paths_str],
        stdout=subprocess.PIPE,
    ) as process:
        # pylint: disable=use-yield-from
        for chunk in iter(lambda: process.stdout.read(STREAM_CHUNK_SIZE), b""):
            yield chunk


def download_file(data_product_file_paths: list[pathlib.Path]) -> StreamingResponse:
    """
    Streams the contents of the specified file path as a download response compressed as a tar
    archive.

    Args:
        data_product_file_path (pathlib.Path): The path to the file to be downloaded.

    Returns:
        fastapi.StreamingResponse: A streaming response object representing the compressed file
        content.
    """
    return StreamingResponse(
        generate_data_stream(data_product_file_paths),
        media_type="application/x-tar",
    )


def verify_file_path(parent_path: pathlib.Path) -> None:
    """
    Verifies if the parent directory of the file path exists.

    Raises:
        FileNotFoundError: If the parent directory doesn't exist.
    """
    if not parent_path.exists():
        raise FileNotFoundError(f"Parent directory not found: {parent_path}")


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


def compare_integer(operand: int, operator: str, comparator: int | list[int]) -> bool:
    """
    Compares an integer operand with a comparator value(s) based on a specified operator.

    Args:
        operand (int): The integer data value to compare.
        operator (str): The comparison operator to use. Supported operators are:
        - "equals": Checks if the operand is equal to the comparator (int).
        - "isAnyOf": Checks if the operand is present in the comparator list (list[int]).
        comparator (int | list[int]): The value or list of values to compare the operand against.

    Returns:
        bool: True if the comparison succeeds based on the operator, False otherwise.

    Raises:
        ValueError: If an unsupported operator is provided.
    """

    match operator:
        case "equals":
            if operand == comparator:
                return True
        case "isAnyOf":
            if str(operand) in str(comparator).split(","):
                return True
        case _:
            raise ValueError(f"Unsupported filter operator for integers: {operator}")
    return False


# pylint: disable=too-many-return-statements
def filter_strings(operand: str, operator: str, comparator: str) -> bool:
    """
    This function filters strings based on a provided operator and comparator.

    Args:
        operand: The string to be filtered.
        operator: The operation to perform on the string. Supported operators are:
            - "contains": Checks if the comparator substring is present within the operand string.
            - "equals": Checks for exact string equality between operand and comparator.
            - "startsWith": Checks if the operand string starts with the comparator substring.
            - "endsWith": Checks if the operand string ends with the comparator substring.
            - "isAnyOf": Checks if the operand string is present within a comma-separated list of
            values in the comparator string.
        comparator: The value to compare the operand string against.

    Returns:
        True if the filtering condition based on the operator and comparator is met, False
        otherwise.

    Raises:
        ValueError: If an unsupported operator is provided.
    """
    if operand is None:
        operand = ""  # Handle None values as empty strings
    match operator:
        case "contains":
            if comparator in str(operand):
                return True
        case "equals":
            if operand == comparator:
                return True
        case "startsWith":
            if str(operand).startswith(comparator):
                return True
        case "endsWith":
            if str(operand).endswith(comparator):
                return True
        case "isAnyOf":
            if operand in comparator.split(","):
                return True
        case _:
            raise ValueError(f"Unsupported filter operator for strings: {operator}")
    return False


def filter_datetimes(
    operand: datetime, operator: str, comparator: datetime
) -> list[dict[str, Any]]:
    """
    This function filters datetime objects based on a provided operator and comparator datetime
    object.

    Args:
        operand: The datetime object to be filtered.
        operator: The operation to perform on the datetime object. Supported operators are:
            - "equals": Checks for exact equality between the operand datetime and the comparator
            datetime.
            - "greaterThan": Checks if the operand datetime is strictly greater than the
            comparator datetime.
            - "lessThan": Checks if the operand datetime is strictly less than the comparator
            datetime.

    Returns:
        A list containing a single dictionary if the filtering condition is met (empty list
        otherwise). The dictionary contains a single key "item" with the matching datetime object
        as its value.

    Raises:
        ValueError: If an unsupported operator is provided or if the datetime string representation
        in the comparator cannot be parsed.
    """
    if operand is None:
        return False  # Skip None values
    match operator:
        case "equals":
            if operand == comparator:
                return True
        case "greaterThan":
            if operand >= comparator:
                return True
        case "lessThan":
            if operand <= comparator:
                return True
        case _:
            raise ValueError(f"Unsupported filter operator for datetimes: {operator}")
    return False


def filter_by_item(
    data: list[dict[str, Any]], field: str, operator: str, comparator: Any
) -> list[dict[str, Any]]:
    """
    Filters a list of dictionaries based on a single field, operator, and value.

    Args:
        data: The list of dictionaries to filter.
        field: The field name to filter on.
        operator: The filtering operation to perform (e.g., "contains", "equals", "startsWith",
        "endsWith", "isAnyOf").
        comparator: The value to compare with the field.

    Raises:
        ValueError: If an unsupported filter operator is provided.

    Returns:
        A new list containing only the dictionaries that match the filter criteria.
    """

    filtered_data: list[dict[str, Any]] = []

    for item in data:
        try:
            operand = item.get(field)  # “operand” (the data value)

            if isinstance(comparator, int):
                if compare_integer(operand, operator, comparator):
                    filtered_data.append(item)
            elif isinstance(comparator, datetime):
                try:
                    date_value = parse_valid_date(operand, "%Y-%m-%d")
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.error("Error, invalid date=%s", exception)
                    continue
                if filter_datetimes(date_value, operator, comparator):
                    filtered_data.append(item)
            else:
                if filter_strings(operand, operator, comparator):
                    filtered_data.append(item)
        except ValueError as error:
            logging.error("Failed to filter on item %s with error %s", str(item), error)

    return filtered_data


def has_nested_status(operand: dict | list, searched_key: str, comparator: str) -> bool:
    """
    Searches for a nested key-value pair within a dictionary or list structure.

    Args:
        operand (dict | list): The dictionary or list to search within.
        searched_key (str): The key to search for within nested dictionaries.
        comparator (str): The value to search for within the nested dictionary
                              associated with the searched_key.

    Returns:
        bool: True if the key-value pair is found nested within the item, False otherwise.

    Raises:
        TypeError: If the `item` is not a dictionary or list.
    """

    if not isinstance(operand, (dict, list)):
        raise TypeError(f"Expected item to be a dictionary or list, got {type(operand)}")

    for key, value in operand.items() if isinstance(operand, dict) else enumerate(operand):
        if key and value:
            if searched_key in str(key) and comparator in str(value):
                return True

            if isinstance(value, (dict, list)):
                if has_nested_status(value, searched_key, comparator):
                    return True

    return False


def filter_by_key_value_pair(
    data: list[dict[str, Any]], key_value_pairs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
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
        searched_key = key_value_pair.get("keyPair", "")
        searched_value = key_value_pair.get("valuePair", "")

        filtered_data = [
            item
            for item in filtered_data
            if has_nested_status(
                operand=item, searched_key=searched_key, comparator=searched_value
            )
        ]

    return filtered_data


def parse_valid_date(date_string: str, expected_format: str) -> datetime:
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
        return datetime.strptime(date_string, expected_format)
    except ValueError as error:
        logging.error("Invalid date format: %s. Expected format: %s", date_string, expected_format)
        raise error
    except TypeError as error:
        logging.error("Invalid date_string: %s", date_string)
        raise error


def verify_persistent_storage_file_path(path: pathlib.Path) -> None:
    """
    Verifies if the given path is a valid directory for persistent storage.

    Raises:
        FileNotFoundError: If the path does not exist.
        NotADirectoryError: If the path is not a directory.
        OSError: If the path is a symbolic link.

    Args:
        path: The full path to the directory to be verified.
    """

    if not path.exists():
        raise FileNotFoundError(f"Directory does not exist: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Invalid directory path: {path}")

    if path.is_symlink():
        raise OSError(f"Symbolic links are not supported: {path}")


def walk_folder(folder_path: str) -> Generator[str, None, None]:
    """
    Walks through a directory and its subdirectories recursively, yielding the full path of each
    file.

    Args:
        folder_path: The path to the root directory to start the walk from.

    Yields:
        The full path of each file found during the walk.
    """
    for root, _, files in os.walk(folder_path):
        for file in files:
            yield os.path.join(root, file)
