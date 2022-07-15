"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse


app = FastAPI()
filename = ""

PERSISTANTSTORAGEPATH = (
    "/mnt/c/Users/Andre/ska_repos/ska-sdp-data-product-api/files/"
)

origins = [
    "http://localhost",
    "http://localhost:3300",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def getfilenames(path):
    """Work in progress"""
    filelist = []
    
    currentDirectory = ""
    id = 0
    for _root, dirs, files in os.walk(path):
        for file in files:
            # filelist.append(os.path.join(currentDirectory,file))
            fileListItem = {}
            fileListItem.update({'id':id})
            fileListItem.update({'filename':os.path.join(currentDirectory,file)})
            filelist.append(fileListItem)
            # print(str(fileListItem))
            id = id+1
            # print("This is a File:"+file)
        for directory in dirs:
            currentDirectory = currentDirectory + directory+"/"
            # filelist.append(directory)
            # print("This is a folder:"+directory)
    return filelist
# getfilenames("/mnt/c/Users/Andre/ska_repos/ska-sdp-data-product-api/files/")

def downloadfile(filename):
    """Work in progress"""
    file_path = os.path.join(PERSISTANTSTORAGEPATH, filename)
    if os.path.exists(file_path):
        print(file_path)
        return FileResponse(file_path, media_type='application/octet-stream',filename=filename)
    return {"error": "File not found!"}

@app.get("/")
async def root():
    """Work in progress"""
    return {"message": "Hello World"}


@app.get("/filelist")
def index():
    """Work in progress"""
    return {"filelist": getfilenames(PERSISTANTSTORAGEPATH)}


@app.get("/download/{filename}")
async def download(filename):
    """Work in progress"""
    return (downloadfile(filename))
