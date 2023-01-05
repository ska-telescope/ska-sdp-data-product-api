"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import os
import pathlib
import shutil

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# pylint: disable=no-name-in-module
from pydantic import BaseModel
from starlette.config import Config
from starlette.responses import FileResponse

config = Config(".env")
PERSISTANT_STORAGE_PATH: str = config(
    "PERSISTANT_STORAGE_PATH",
    default="./tests/test_files",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL",
    default="http://localhost",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT",
    default="8100",
)

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str


class DataProductIndex:
    """This class contains the list of data products with their file names,
    paths and an ID for each"
    """

    def __init__(self, root_tree_item_id, tree_data):
        self.tree_item_id = root_tree_item_id
        self.tree_data = tree_data


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost" + ":" + REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT,
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL,
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL
    + ":"
    + REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def getfilenames(path, data_product_index: DataProductIndex):
    """getfilenames itterates through a folder specified with the path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    tree_data = {
        "id": data_product_index.tree_item_id,
        "name": os.path.basename(path),
        "relativefilename": str(pathlib.Path(*pathlib.Path(path).parts[3:])),
    }
    if data_product_index.tree_item_id == "root":
        data_product_index.tree_item_id = 0
    data_product_index.tree_item_id = data_product_index.tree_item_id + 1
    if os.path.isdir(path):
        tree_data["type"] = "directory"
        tree_data["children"] = [
            getfilenames(os.path.join(path, x), data_product_index)
            for x in os.listdir(path)
        ]
    else:
        tree_data["type"] = "file"
    return tree_data


def downloadfile(relative_path_name):
    """Work in progress"""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, relative_path_name
    )

    if os.path.exists(persistant_file_path):
        if os.path.isdir(persistant_file_path):
            shutil.make_archive(
                persistant_file_path, "zip", persistant_file_path
            )
            return FileResponse(
                persistant_file_path + ".zip",
                media_type="application/zip",
                filename=relative_path_name,
            )
        return FileResponse(
            persistant_file_path,
            media_type="application/octet-stream",
            filename=relative_path_name,
        )
    raise HTTPException(
        status_code=404,
        detail=f"File with name {persistant_file_path} not found",
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
    return downloadfile(relative_file_name.relativeFileName)
