"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.config import Config
from starlette.responses import FileResponse

config = Config(".env")
PERSISTANT_STORAGE_PATH: str = config(
    "PERSISTANT_STORAGE_PATH",
    default="../files/",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL",
    default="http://localhost",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT",
    default="3300",
)

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


def getfilenames(path):
    """getfilenames itterates through a folder specified with the path
    parameter, and returns a list of files and their relative paths as
    well as an index used.

    This is done as a first proof of concept, and will need to be updated
    to actual use cases"""
    filelist = []

    current_directory = ""
    file_id = 0
    for _root, dirs, files in os.walk(path):
        for file in files:
            file_list_item = {}
            file_list_item.update({"id": file_id})
            file_list_item.update(
                {"filename": os.path.join(current_directory, file)}
            )
            filelist.append(file_list_item)
            file_id = file_id + 1
        for directory in dirs:
            file_list_item = {}
            file_list_item.update({"id": file_id})
            file_list_item.update(
                {"filename": os.path.join(current_directory, directory)}
            )
            current_directory = current_directory + directory + "/"
            filelist.append(file_list_item)
            file_id = file_id + 1
    return filelist


def downloadfile(filename):
    """Work in progress"""
    file_path = os.path.join(PERSISTANT_STORAGE_PATH, filename)
    if os.path.exists(file_path):
        return FileResponse(
            file_path, media_type="application/octet-stream", filename=filename
        )
    raise HTTPException(
        status_code=404, detail=f"File with name {filename} not found"
    )


@app.get("/")
async def root():
    """An enpoint that just returns Hello World"""
    return {"message": "Hello World"}


@app.get("/filelist")
def index():
    """This API endpoint returns a list of all the files in the
    PERSISTANT_STORAGE_PATH in the following format {"filelist":
    [{"id":0,"filename":"file1.extentions"},{"id":1,"filename":"
    Subfolder1/SubSubFolder/file2.extentions"}]}
    """
    return {"filelist": getfilenames(PERSISTANT_STORAGE_PATH)}


@app.get("/download/{filename}")
async def download(filename):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(filename)
