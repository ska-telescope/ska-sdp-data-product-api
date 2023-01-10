"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import io
import os
import pathlib
import zipfile
from pathlib import Path
import ast

from fastapi import HTTPException, Response

# pylint: disable=no-name-in-module
from pydantic import BaseModel
from starlette.responses import FileResponse

from ska_sdp_data_product_api.core.settings import PERSISTANT_STORAGE_PATH, app

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str
    fileName: str


class DataProductIndex:
    """This class contains the list of data products with their file names,
    paths and an ID for each"
    """

    def __init__(self, root_tree_item_id, tree_data):
        self.tree_item_id = root_tree_item_id
        self.tree_data:dict = tree_data

    def add_tree_data(self, new_data):
        """Merge current dict with new data"""
        print("\nOriginal Data : %s", self.tree_data)
        print("\nNew data : %s", new_data)
        # self.tree_data = self.tree_data | new_data
        if self.tree_data["children"] == [] :
            self.tree_data["children"] = new_data
            # self.tree_data = ast.literal_eval(str(self.tree_data) + "," + str(new_data))
        else:
            self.tree_data["children"] = self.tree_data["children"], new_data
        print("\nCombined data %s",self.tree_data)

def verify_file_path(file_path):
    """Test if the file path exists"""
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File path with name {file_path} not found",
        )


def getfilenames(
    persistent_storage_path, file_index: DataProductIndex
):
    """getfilenames itterates through a folder specified with the path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    verify_file_path(persistent_storage_path)

    tree_data = {
        "id": file_index.tree_item_id,
        "name": os.path.basename(persistent_storage_path),
        "relativefilename": str(
            pathlib.Path(*pathlib.Path(persistent_storage_path).parts[2:])
        ),
    }
    if file_index.tree_item_id == "root":
        file_index.tree_item_id = 0
    file_index.tree_item_id = file_index.tree_item_id + 1
    if os.path.isdir(persistent_storage_path):
        tree_data["type"] = "directory"
        tree_data["children"] = [
            getfilenames(
                os.path.join(persistent_storage_path, x), file_index
            )
            for x in os.listdir(persistent_storage_path)
        ]
    else:
        tree_data["type"] = "file"
    return tree_data

def getdataproductlist(persistent_storage_path, file_index:DataProductIndex, data_product_tree_index):
    """getdataproductlist itterates through a folder specified with the path
    parameter, and returns a list of all the data products and a relative 
    paths and adds an index to the list.
    A folder is considred a data product if the folder contains a
    file named 'ska-data-product.yaml'
    """
    verify_file_path(persistent_storage_path)
    metadata_file_name = "ska-data-product.yaml"

    # If the path points to a directory
    if os.path.isdir(persistent_storage_path):
        # for each directory in the persistent_storage_path
        for file in os.listdir(persistent_storage_path):
            metadata_file_path = os.path.join(str(persistent_storage_path), str(metadata_file_name))
            #if the directory contains a metadatafile
            if file == metadata_file_name:
                if os.path.isdir(persistent_storage_path):
                    print("Metadata Found in folder %s", persistent_storage_path)
                    file_index.add_tree_data(getfilenames(
                        persistent_storage_path, data_product_tree_index
                    ))
            else:
                getdataproductlist(
                os.path.join(persistent_storage_path, file), file_index, data_product_tree_index)

    return file_index.tree_data


def downloadfile(relative_path_name):
    """Work in progress"""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, relative_path_name.relativeFileName
    )
    # Not found
    verify_file_path(persistant_file_path)
    # File
    if not os.path.isdir(persistant_file_path):
        return FileResponse(
            persistant_file_path,
            media_type="application/octet-stream",
            filename=relative_path_name.relativeFileName,
        )
    # Directory
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


@app.get("/filelistoriginal")
def index():
    """This API endpoint returns a list of all the files in the
    PERSISTANT_STORAGE_PATH in the following format {"id":"root",
    "name":"test_files","relativefilename":".","type":"directory",
    "children":[{"id":1,"name":"product","relativefilename":".",
    "type":"directory","children":[...]}]}
    """
    file_index = DataProductIndex(
        root_tree_item_id="root", tree_data={}
    )
    file_index.tree_data = getfilenames(
        PERSISTANT_STORAGE_PATH, file_index
    )
    print("\n\nfinal tree_data: %s", file_index.tree_data)
    return file_index.tree_data

@app.get("/filelist")
def index():
    """This API endpoint returns a list of all the data products 
    in the PERSISTANT_STORAGE_PATH in the following format 
    {"dataproductlist":[{"id":0,"filename":"file1.extentions"},{"id":1,
    "filename":"Subfolder1/SubSubFolder/file2.extentions"}]}
    """
    tree_data = {
        "id": "root",
        "name": "Data Products",
        "relativefilename": "",
        "type": "directory",
        "children": []
    }

    file_index = DataProductIndex(
        root_tree_item_id="root", tree_data=tree_data
    )

    data_product_tree_index = DataProductIndex(
        root_tree_item_id=1, tree_data={}
    )
    file_index.tree_data = getdataproductlist(
        PERSISTANT_STORAGE_PATH, file_index, data_product_tree_index)



    print("\n\nfinal tree_data: %s", tree_data)
    return tree_data

@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name)
