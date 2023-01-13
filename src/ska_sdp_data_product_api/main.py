"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import io
import json
import os
import pathlib
import zipfile
from pathlib import Path

import yaml
from fastapi import HTTPException, Response

# pylint: disable=no-name-in-module
from pydantic import BaseModel
from starlette.responses import FileResponse

from ska_sdp_data_product_api.core.settings import (
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
    app,
)

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str = "Untitled"
    fileName: str


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


def getfilenames(storage_path, file_index: TreeIndex, metadata_file):
    """getfilenames itterates through a folder specified with the storage_path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    verify_file_path(storage_path)

    # Add the details of the current storage_path to the tree data
    tree_data = {
        "id": file_index.tree_item_id,
        "name": os.path.basename(storage_path),
        "metadatafile": metadata_file,
        "relativefilename": str(
            pathlib.Path(*pathlib.Path(storage_path).parts[2:])
        ),
    }
    # The first entry in the tree indicates it is the root of the tree (usd to
    # render in JS), there after, incriment the index numericaly.
    if file_index.tree_item_id == "root":
        file_index.tree_item_id = 0
    file_index.tree_item_id = file_index.tree_item_id + 1
    # If the current storage_path is a directory, add its details and children
    # by calling this funcion (getfilenames) with the path to the children
    if os.path.isdir(storage_path):
        tree_data["type"] = "directory"
        tree_data["children"] = [
            getfilenames(
                os.path.join(storage_path, x), file_index, metadata_file
            )
            for x in os.listdir(storage_path)
        ]
    else:
        tree_data["type"] = "file"
    return tree_data


def getdataproductlist(storage_path, file_index: TreeIndex):
    """getdataproductlist itterates through a folder specified with the path
    parameter, and returns a list of all the data products and their relative
    paths and adds an index to the list.
    A folder is considred a data product if the folder contains a
    file named specified in the env variable METADATA_FILE_NAME.
    """
    verify_file_path(storage_path)

    # Test if the path points to a directory
    if os.path.isdir(storage_path):
        # For each file in the directory,
        files = os.listdir(storage_path)
        # test if the directory contains a metadatafile
        if METADATA_FILE_NAME in files:
            # If it contains the metadata file, create a new child
            # element for the data product dict.
            metadata_file = Path(storage_path).joinpath(METADATA_FILE_NAME)
            if file_index.tree_item_id == "root":
                file_index.tree_item_id = 0
            file_index.append_children(
                getfilenames(storage_path, file_index, metadata_file)
            )
        else:
            # If it is not a data product, enter the folder and repeat
            # this test.
            for file in os.listdir(storage_path):
                getdataproductlist(
                    os.path.join(storage_path, file), file_index
                )

    return file_index.tree_data


def downloadfile(relative_path_name):
    """This function returns a response that can be used to download a file
    pointed to by the relative_path_name"""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, relative_path_name.relativeFileName
    )
    # Not found
    verify_file_path(persistant_file_path)
    # If relative_path_name points to a file, return a FileResponse
    if not os.path.isdir(persistant_file_path):
        return FileResponse(
            persistant_file_path,
            media_type="application/octet-stream",
            filename=relative_path_name.relativeFileName,
        )
    # If relative_path_name points to a directory, retrun a zipfile data
    # stream response
    zip_file_buffer = io.BytesIO()
    with zipfile.ZipFile(
        zip_file_buffer, "a", zipfile.ZIP_DEFLATED, False
    ) as zip_file:
        for dir_name, _, files in os.walk(persistant_file_path):
            for filename in files:
                file = os.path.join(dir_name, filename)
                relative_file = Path(str(file)).relative_to(
                    Path(persistant_file_path)
                )
                zip_file.write(file, arcname=relative_file)
    headers = {
        "Content-Disposition": f'attachment; filename="\
            {relative_path_name.relativeFileName}.zip"'
    }
    return Response(
        zip_file_buffer.getvalue(),
        media_type="application/zip",
        headers=headers,
    )


def loadmetadata(path_to_selected_file):
    """This function returns a response that can be used to download a file
    pointed to by the relative_path_name"""
    # Not found
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, path_to_selected_file.relativeFileName
    )

    verify_file_path(persistant_file_path)

    with open(
        persistant_file_path, "r", encoding="utf-8"
    ) as metadata_yaml_file:
        metadata_yaml_object = yaml.safe_load(
            metadata_yaml_file
        )  # yaml_object will be a list or a dict
    metadata_json = json.dumps(metadata_yaml_object)
    return metadata_json


def finddataproductmetadatafile(path_to_selected_file):
    """This function returns the metadata file path that is applicable to
    the selected data product. This metadata file is either in the same
    folder as the selected file, or a parent directory that form part of
    the data product.
    """
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, path_to_selected_file.relativeFileName
    )
    # if path_to_selected_file points to metadata file
    posix_path = Path(persistant_file_path)
    if posix_path.name == METADATA_FILE_NAME:
        return posix_path
    # if the path_to_selected_file points to folder without a metadata file,
    # go 1 folder up and test again.
    while verify_file_path(posix_path) and str(posix_path) != ".":
        metadata_file = posix_path.joinpath(METADATA_FILE_NAME)
        print("looking for metadata_file: %s", metadata_file)
        if metadata_file.is_file():
            print(metadata_file)
            return metadata_file
        posix_path = posix_path.parents[0]


@app.get("/ping")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return {"ping": "The application is running"}


@app.get("/dataproductlist")
def index_data_products():
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """

    file_index = TreeIndex(
        root_tree_item_id="root",
        tree_data={
            "id": "root",
            "name": "Data Products",
            "relativefilename": "",
            "type": "directory",
            "children": [],
        },
    )

    file_index.tree_data = getdataproductlist(
        PERSISTANT_STORAGE_PATH, file_index
    )

    return file_index.tree_data


@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name)


@app.post("/dataproductmetadata", response_class=Response)
async def dataproductmetadata(relative_file_name: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return loadmetadata(relative_file_name)
