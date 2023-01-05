"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import io
import os
import pathlib
import zipfile

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
        self.tree_data = tree_data


def verify_file_path(file_path):
    """Test if the file path exists"""
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File path with name {file_path} not found",
        )


def getfilenames(
    persistent_storage_path, data_product_index: DataProductIndex
):
    """getfilenames itterates through a folder specified with the path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    verify_file_path(persistent_storage_path)

    tree_data = {
        "id": data_product_index.tree_item_id,
        "name": os.path.basename(persistent_storage_path),
        "relativefilename": str(
            pathlib.Path(*pathlib.Path(persistent_storage_path).parts[3:])
        ),
    }
    if data_product_index.tree_item_id == "root":
        data_product_index.tree_item_id = 0
    data_product_index.tree_item_id = data_product_index.tree_item_id + 1
    if os.path.isdir(persistent_storage_path):
        tree_data["type"] = "directory"
        tree_data["children"] = [
            getfilenames(
                os.path.join(persistent_storage_path, x), data_product_index
            )
            for x in os.listdir(persistent_storage_path)
        ]
    else:
        tree_data["type"] = "file"
    return tree_data


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
            zip_file.write(dir_name)
            for filename in files:
                zip_file.write(os.path.join(dir_name, filename))
    headers = {
        "Content-Disposition": f'attachment; filename="\
            {relative_path_name.relativeFileName}"'
    }
    return Response(
        zip_file_buffer.getvalue(),
        media_type="application/zip",
        headers=headers,
    )


@app.get("/ping")
async def root():
    """An enpoint that just returns ping live"""
    return {"ping": "live"}


@app.get("/filelist")
def index():
    """This API endpoint returns a list of all the files in the
    PERSISTANT_STORAGE_PATH in the following format {"filelist":
    [{"id":0,"filename":"file1.extentions"},{"id":1,"filename":"
    Subfolder1/SubSubFolder/file2.extentions"}]}
    """
    data_product_index = DataProductIndex(
        root_tree_item_id="root", tree_data={}
    )
    data_product_index.tree_data = getfilenames(
        PERSISTANT_STORAGE_PATH, data_product_index
    )
    return data_product_index.tree_data


@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name)
