"""Module to insert data into Elasticsearch instance."""
import datetime
import io
import json
import os
import pathlib
import zipfile
from typing import Optional

# pylint: disable=no-name-in-module
import pydantic
import yaml
from fastapi import HTTPException, Response
from pydantic import BaseModel

# pylint: disable=no-name-in-module
from starlette.responses import FileResponse

from ska_sdp_dataproduct_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
)

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Class object to do file path validation"""

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
    def relative_path_name_validator(cls, relative_path):
        """verify that relative_path file path"""
        path = PERSISTANT_STORAGE_PATH.joinpath(relative_path)
        verify_file_path(path)
        return relative_path

    @pydantic.validator("fullPathName", pre=True)
    @classmethod
    def full_path_name_validator(cls, full_path_name, values):
        """verify that fullPathName file path, if not given, derive it from
        the relativePathName and PERSISTANT_STORAGE_PATH"""
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


def downloadfile(file_object: FileUrl):
    """This function returns a response that can be used to download a file
    pointed to by the file_object"""
    # If file_object points to a file, return a FileResponse
    if not os.path.isdir(file_object.fullPathName):
        return FileResponse(
            file_object.fullPathName,
            media_type="application/octet-stream",
            filename=file_object.fileName,
        )
    # If file_object points to a directory, retrun a zipfile data
    # stream response
    zip_file_buffer = io.BytesIO()
    with zipfile.ZipFile(
        zip_file_buffer, "a", zipfile.ZIP_DEFLATED, False
    ) as zip_file:
        for dir_name, _, files in os.walk(file_object.fullPathName):
            for filename in files:
                file = os.path.join(dir_name, filename)
                relative_file = pathlib.Path(str(file)).relative_to(
                    pathlib.Path(file_object.fullPathName)
                )
                zip_file.write(file, arcname=relative_file)
    headers = {
        "Content-Disposition": f'attachment; filename="\
            {file_object.relativePathName}.zip"'
    }
    return Response(
        zip_file_buffer.getvalue(),
        media_type="application/zip",
        headers=headers,
    )


def verify_file_path(file_path):
    """Test if the file path exists"""
    if not pathlib.Path(file_path).exists():
        raise HTTPException(
            status_code=404,
            detail=f"File path with name '{file_path}' not found",
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
    file_object: FileUrl,
    dataproduct_file_name="",
    metadata_file_name="",
):
    """This function loads the content of a yaml file and return it as
    json."""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, file_object.relativePathName
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


def ingestmetadatafiles(metadata_store_object, full_path_name: str):
    """This method runs through a volume and add all the data products to
    the metadata_list if the store"""
    # Test if the path points to a directory
    if os.path.isdir(full_path_name) and not os.path.islink(full_path_name):
        # For each file in the directory,
        files = os.listdir(full_path_name)
        # test if the directory contains a metadatafile
        if METADATA_FILE_NAME in files:
            # If it contains the metadata file add it to the index
            dataproduct_file_name = relativepath(full_path_name)
            metadata_file = pathlib.Path(full_path_name).joinpath(
                METADATA_FILE_NAME
            )
            metadata_file_name = FileUrl
            metadata_file_name.relativePathName = relativepath(metadata_file)
            metadata_file_json = loadmetadatafile(
                metadata_file_name,
                dataproduct_file_name,
                metadata_file_name.relativePathName,
            )
            metadata_store_object.insert_metadata(metadata_file_json)
        else:
            # If it is not a data product, enter the folder and repeat
            # this test.
            for file in os.listdir(full_path_name):
                ingestmetadatafiles(
                    metadata_store_object, os.path.join(full_path_name, file)
                )
