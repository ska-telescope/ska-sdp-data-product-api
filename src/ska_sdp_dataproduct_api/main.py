"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import datetime
import io
import json
import os
import pathlib
import zipfile
from collections.abc import MutableMapping
from pathlib import Path

import yaml
from fastapi import HTTPException, Response

# pylint: disable=no-name-in-module
from pydantic import BaseModel
from starlette.responses import FileResponse

from ska_sdp_dataproduct_api.core.settings import (
    ES_HOST,
    METADATA_FILE_NAME,
    PERSISTANT_STORAGE_PATH,
    app,
)
from ska_sdp_dataproduct_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)

# pylint: disable=too-few-public-methods

metadata_store = ElasticsearchMetadataStore()
metadata_store.connect(hosts=ES_HOST)


class FileUrl(BaseModel):
    """Relative path and file name"""

    relativeFileName: str = "Untitled"
    fileName: str


class SearchParametersClass(BaseModel):
    """Class for defining search parameters"""

    start_date: str = "2020-01-01"
    end_date: str = "2100-01-01"
    key_pair: str = ""


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


def downloadfile(relative_path_name):
    """This function returns a response that can be used to download a file
    pointed to by the relative_path_name"""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, relative_path_name.relativeFileName
    )
    # Test is not found
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
    path_to_selected_file: FileUrl,
    dataproduct_file_name="",
    metadata_file_name="",
):
    """This function loads the content of a yaml file and return it as
    json."""
    persistant_file_path = os.path.join(
        PERSISTANT_STORAGE_PATH, path_to_selected_file.relativeFileName
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


def generatemetadatakeyslist(d, parent_key="", sep="_", ignore_keys=[]):
    """Given a nested dict, return the flattened list of keys"""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(
                generatemetadatakeyslist(
                    v, new_key, sep=sep, ignore_keys=ignore_keys
                )
            )
        else:
            if new_key not in ignore_keys:
                items.append(new_key)
    return items


def createmetadatafilelist(
    metadata_file_json,
):
    """Create the metadata list"""
    if metadata_store.es_search_enabled:
        metadata_store.insert_metadata(
            metadata_file_json,
        )
    else:
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = generatemetadatakeyslist(
            metadata_file, "", ".", ["files"]
        )

        metadata_store.update_dataproduct_list(
            metadata_file=metadata_file, query_key_list=query_key_list
        )


def relativepath(absolute_path):
    """This function returns the relative path of an absolute path where the
    absolute path = PERSISTANT_STORAGE_PATH + relative_path"""
    persistant_storage_path_len = len(Path(PERSISTANT_STORAGE_PATH).parts)
    relative_path = str(
        pathlib.Path(
            *pathlib.Path(absolute_path).parts[(persistant_storage_path_len):]
        )
    )
    return relative_path


def ingestmetadatafiles(storage_path: str):
    """This function runs through a volume and add all the data products to the
    metadata_store or into the metadata_list if the store is not available"""
    if verify_file_path(storage_path):
        # Test if the path points to a directory
        if os.path.isdir(storage_path) and not os.path.islink(storage_path):
            # For each file in the directory,
            files = os.listdir(storage_path)
            # test if the directory contains a metadatafile
            if METADATA_FILE_NAME in files:
                # If it contains the metadata file add it to the index
                dataproduct_file_name = relativepath(storage_path)
                metadata_file = Path(storage_path).joinpath(METADATA_FILE_NAME)
                metadata_file_name = FileUrl
                metadata_file_name.relativeFileName = relativepath(
                    metadata_file
                )
                metadata_file_json = loadmetadatafile(
                    metadata_file_name,
                    dataproduct_file_name,
                    metadata_file_name.relativeFileName,
                )
                createmetadatafilelist(metadata_file_json)
            else:
                # If it is not a data product, enter the folder and repeat
                # this test.
                for file in os.listdir(storage_path):
                    ingestmetadatafiles(os.path.join(storage_path, file))
        return ""
    return "Metadata ingested"


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    status = {
        "API_running": True,
        "Search_enabled": metadata_store.es_search_enabled,
    }
    return status


@app.get("/updatesearchindex")
def update_search_index():
    """This endpoint triggers the ingestion of metadata"""
    metadata_store.clear_indecise()
    return ingestmetadatafiles(PERSISTANT_STORAGE_PATH)


@app.post("/dataproductsearch", response_class=Response)
def data_products_search(search_parameters: SearchParametersClass):
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    if not metadata_store.es_search_enabled:
        metadata_store.connect(hosts=ES_HOST)
        if not metadata_store.es_search_enabled:
            raise HTTPException(
                status_code=503, detail="Elasticsearch not found"
            )
    filtered_data_product_list = metadata_store.search_metadata(
        start_date=search_parameters.start_date,
        end_date=search_parameters.end_date,
        metadata_key=search_parameters.key_pair.split(":")[0],
        metadata_value=search_parameters.key_pair.split(":")[1],
    )
    return filtered_data_product_list


@app.get("/dataproductlist", response_class=Response)
def data_products_list():
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    if not metadata_store.es_search_enabled:
        metadata_store.metadata_list = []
        ingestmetadatafiles(PERSISTANT_STORAGE_PATH)
    return json.dumps(metadata_store.metadata_list)


@app.post("/download")
async def download(relative_file_name: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(relative_file_name)


@app.post("/dataproductmetadata", response_class=Response)
async def dataproductmetadata(relative_file_name: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return loadmetadatafile(relative_file_name)
