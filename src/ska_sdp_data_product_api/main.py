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
    default="3300",
)

# pylint: disable=too-few-public-methods


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str


app = FastAPI()

origins = [
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

TREE_ITEM_ID = "root"


def getfilenames(path):
    """getfilenames itterates through a folder specified with the path
    parameter, and returns a list of files and their relative paths as
    well as an index used.
    """
    # pylint: disable=global-statement
    global TREE_ITEM_ID
    tree_data = {
        "id": TREE_ITEM_ID,
        "name": os.path.basename(path),
        "relativefilename": str(pathlib.Path(*pathlib.Path(path).parts[2:])),
    }
    if TREE_ITEM_ID == "root":
        TREE_ITEM_ID = 0
    TREE_ITEM_ID = TREE_ITEM_ID + 1
    if os.path.isdir(path):
        tree_data["type"] = "directory"
        tree_data["children"] = [
            getfilenames(os.path.join(path, x)) for x in os.listdir(path)
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
        detail=f"File with name {relative_path_name} not found",
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
    # pylint: disable=global-statement
    global TREE_ITEM_ID
    TREE_ITEM_ID = "root"
    return getfilenames(PERSISTANT_STORAGE_PATH)


@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name.relativeFileName)
