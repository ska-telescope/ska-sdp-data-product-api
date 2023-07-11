"""Module to insert data into Elasticsearch instance."""
import datetime
import gzip
import json
import pathlib
import tarfile
from io import BytesIO
from typing import Optional

# pylint: disable=no-name-in-module
import pydantic
import requests
import yaml
from fastapi import HTTPException, Response
from pydantic import BaseModel

from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
    VERSION,
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
        """This mothod update the timestamp of the last time that data was
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


def downloadfile(file_object: FileUrl):
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


def relativepath(absolute_path):
    """This function returns the relative path of an absolute path where the
    absolute path = PERSISTANT_STORAGE_PATH + relative_path"""
    persistant_storage_path_len = len(PERSISTANT_STORAGE_PATH.parts)
    relative_path = str(
        pathlib.Path(
            *pathlib.Path(absolute_path).parts[(persistant_storage_path_len):]
        )
    )
    return pathlib.Path(relative_path)


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


def loadmetadatafile(file_object: FileUrl):
    """This function loads the content of a yaml file and return it as
    json."""
    if (file_object.fullPathName).is_file():
        with open(
            file_object.fullPathName, "r", encoding="utf-8"
        ) as metadata_yaml_file:
            metadata_yaml_object = yaml.safe_load(
                metadata_yaml_file
            )  # yaml_object will be a list or a dict

        # abort if metadata is empty
        if metadata_yaml_object is None:
            return {}

        # abort if metadata does not contain an execution_block attribute
        if "execution_block" not in metadata_yaml_object:
            return {}

        metadata_date = getdatefromname(
            metadata_yaml_object["execution_block"]
        )
        metadata_yaml_object.update({"date_created": metadata_date})
        metadata_yaml_object.update(
            {"dataproduct_file": str(file_object.relativePathName.parent)}
        )
        metadata_yaml_object.update(
            {"metadata_file": str(file_object.relativePathName)}
        )
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


def add_dataproduct(metadata_list, metadata_file: str, query_key_list):
    """Populate a list of data products and its metadata"""
    data_product_details = {}
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
    #       but add_dataproduct supports multiple query keys
    for query_key in query_key_list:
        query_metadata = find_metadata(metadata_file, query_key)
        if query_metadata is not None:
            data_product_details[query_metadata["key"]] = query_metadata[
                "value"
            ]
    update_dataproduct_list(metadata_list, data_product_details)


def update_dataproduct_list(metadata_list, data_product_details):
    """This function looks if the new data product is in the metadata list,
    if it is, the dataproduct entry is replaced, if it is new, it is appended
    """
    # Adds the first dictionary to the list
    if len(metadata_list) == 0:
        data_product_details["id"] = 1
        metadata_list.append(data_product_details)
        return

    # Iterates through all the items in the metadata_list to see if an
    # entry exist, if it is found, it is replaced, else added to the end.
    for i, product in enumerate(metadata_list):
        if (
            product["execution_block"]
            == data_product_details["execution_block"]
        ):
            data_product_details["id"] = product["id"]
            metadata_list[i] = data_product_details
            return
    data_product_details["id"] = len(metadata_list) + 1
    metadata_list.append(data_product_details)
    return


def ingestmetadatafiles(metadata_store_object, full_path_name: pathlib.Path):
    """This method runs through a volume and add all the data products to
    the metadata_list if the store"""
    # Test if the path points to a directory
    if full_path_name.is_dir() and not full_path_name.is_symlink():
        # For each file in the directory,
        # test if the directory contains a metadatafile
        for sub_path in full_path_name.iterdir():
            if sub_path == sub_path.parent / METADATA_FILE_NAME:
                # If it contains the metadata file add it to the index
                metadata_file = sub_path
                metadata_file_name = FileUrl
                metadata_file_name.fullPathName = (
                    PERSISTANT_STORAGE_PATH.joinpath(
                        relativepath(metadata_file)
                    )
                )
                metadata_file_name.relativePathName = relativepath(
                    metadata_file
                )
                metadata_file_json = loadmetadatafile(
                    metadata_file_name,
                )

                # abort if no metadata was read
                if len(metadata_file_json) == 0:
                    continue

                metadata_store_object.insert_metadata(metadata_file_json)
            else:
                # If it is not a data product, enter the folder and repeat
                # this test.
                for sub_sub_path in full_path_name.iterdir():
                    ingestmetadatafiles(metadata_store_object, sub_sub_path)
