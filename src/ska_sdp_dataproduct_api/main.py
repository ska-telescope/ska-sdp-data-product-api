"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import io
import json
import os
import zipfile
from pathlib import Path

from fastapi import HTTPException, Response

# pylint: disable=no-name-in-module
from starlette.responses import FileResponse

from ska_sdp_dataproduct_api.core.helperfunctions import (
    FileUrl,
    SearchParametersClass,
    loadmetadatafile,
    verify_file_path,
)
from ska_sdp_dataproduct_api.core.settings import (
    ES_HOST,
    PERSISTANT_STORAGE_PATH,
    app,
)
from ska_sdp_dataproduct_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.inmemorystore.inmemorystore import (
    InMemoryDataproductIndex,
)

elk_metadata_store = ElasticsearchMetadataStore()
elk_metadata_store.connect(hosts=ES_HOST)

in_memory_metadata_store = InMemoryDataproductIndex(
    elk_metadata_store.es_search_enabled,
)


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


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    status = {
        "API_running": True,
        "Search_enabled": elk_metadata_store.es_search_enabled,
    }
    return status


@app.get("/reindexdataproducts")
def reindex_data_products():
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    if elk_metadata_store.es_search_enabled:
        elk_metadata_store.reindex()
    else:
        in_memory_metadata_store.reindex()
    return "Metadata store cleared and re-indexed"


@app.post("/dataproductsearch", response_class=Response)
def data_products_search(search_parameters: SearchParametersClass):
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    if not elk_metadata_store.es_search_enabled:
        elk_metadata_store.connect(hosts=ES_HOST)
        if not elk_metadata_store.es_search_enabled:
            raise HTTPException(
                status_code=503, detail="Elasticsearch not found"
            )
    filtered_data_product_list = elk_metadata_store.search_metadata(
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
    return json.dumps(in_memory_metadata_store.metadata_list)


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
