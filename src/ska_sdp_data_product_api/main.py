"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import os

from fastapi import FastAPI

# from fastapi.responses import FileResponse

app = FastAPI()

PERSISTANTSTORAGEPATH = (
    "/mnt/c/Users/Andre/ska_repos/ska-sdp-data-product-api/files/"
)


def getfilenames(path):
    """Work in progress"""
    filelist = []
    for _root, dirs, files in os.walk(path):
        for file in files:
            # filelist.append(os.path.join(root,file))
            filelist.append(file)
        for directory in dirs:
            # filelist.append(os.path.join(root,file))
            filelist.append(directory)
    return filelist


@app.get("/")
async def root():
    """Work in progress"""
    return {"message": "Hello World"}


@app.get("/filelist")
def index():
    """Work in progress"""
    return {"File list": getfilenames(PERSISTANTSTORAGEPATH)}


# @app.get(
#     "/cat",
#     responses={
#         200: {
#             "description": "A picture of a cat.",
#             "content": {
#                 "image/jpeg": {"example": "Just imagine a picture of a cat."}
#             },
#         }
#     },
# )
# def cat():
#     """Work in progress"""
#     file_path = os.path.join(PERSISTANTSTORAGEPATH, "files/cat.jpg")
#     if os.path.exists(file_path):
#         return FileResponse(
#             file_path, media_type="image/jpeg", filename="mycat.jpg"
#         )
#     return {"error": "File not found!"}
