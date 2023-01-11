"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import io
import os
import pathlib
import zipfile
from pathlib import Path

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

    relativeFileName: str
    fileName: str


class TreeIndex:
    """This class contains tree_item_id; an ID field, indicating the next
    tree item id to use, and tree_data dictionary, containing a json object
    that represens a tree structure that can easily be rendered in JS.
    """

    def __init__(self, root_tree_item_id, tree_data):
        self.tree_item_id = root_tree_item_id
        self.tree_data: dict = tree_data

    def add_tree_data(self, new_data):
        """Merge current dict with new data"""
        if self.tree_data["children"] == []:
            self.tree_data["children"] = new_data
        else:
            self.tree_data["children"] = self.tree_data["children"], new_data


def verify_file_path(file_path):
    """Test if the file path exists"""
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File path with name {file_path} not found",
        )


def getfilenames(storage_path, file_index: TreeIndex):
    """getfilenames itterates through a folder specified with the storage_path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    verify_file_path(storage_path)

    # Add the details of the current storage_path to the tree data
    tree_data = {
        "id": file_index.tree_item_id,
        "name": os.path.basename(storage_path),
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
            getfilenames(os.path.join(storage_path, x), file_index)
            for x in os.listdir(storage_path)
        ]
    else:
        tree_data["type"] = "file"
    return tree_data


def getdataproductlist(
    storage_path,
    file_index: TreeIndex,
    data_product_tree_index,
):
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
        for file in os.listdir(storage_path):
            # test if the directory contains a metadatafile
            if file == METADATA_FILE_NAME:
                # If it contains the metadata file, create a new child
                # element for the data product dict.
                file_index.add_tree_data(
                    getfilenames(storage_path, data_product_tree_index)
                )
            else:
                # If it is not a data product, enter the folder and repeat
                # this test.
                getdataproductlist(
                    os.path.join(storage_path, file),
                    file_index,
                    data_product_tree_index,
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


@app.get("/ping")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return {"ping": "The application is running"}


@app.get("/filelist")
def index_files():
    """This API endpoint returns a list of all the files in the
    PERSISTANT_STORAGE_PATH
    """
    file_index = TreeIndex(root_tree_item_id="root", tree_data={})
    file_index.tree_data = getfilenames(PERSISTANT_STORAGE_PATH, file_index)
    return file_index.tree_data


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

    data_product_tree_index = TreeIndex(root_tree_item_id=1, tree_data={})
    file_index.tree_data = getdataproductlist(
        PERSISTANT_STORAGE_PATH, file_index, data_product_tree_index
    )

    return file_index.tree_data


@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name)
