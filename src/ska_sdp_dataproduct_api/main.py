"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import json
import logging

from fastapi import BackgroundTasks, Response
from fastapi.exceptions import HTTPException

from ska_sdp_dataproduct_api.core.helperfunctions import (
    DPDAPIStatus,
    FileUrl,
    SearchParametersClass,
    download_file,
)
from ska_sdp_dataproduct_api.core.settings import DEFAULT_DISPLAY_LAYOUT, ES_HOST, app
from ska_sdp_dataproduct_api.metadatastore.store_factory import select_correct_store_class

logger = logging.getLogger(__name__)

DPD_API_Status = DPDAPIStatus()

store = select_correct_store_class(ES_HOST, DPD_API_Status)


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return DPD_API_Status.status(store.es_search_enabled)


@app.get("/reindexdataproducts", status_code=202)
async def reindex_data_products(background_tasks: BackgroundTasks):
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    background_tasks.add_task(store.reindex)
    logger.info("Metadata store cleared and re-indexed")
    return "Metadata is set to be cleared and re-indexed"


@app.post("/dataproductsearch", response_class=Response)
async def data_products_search(search_parameters: SearchParametersClass):
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    metadata_key_value_pairs = []
    if (
        search_parameters.key_value_pairs is not None
        and len(search_parameters.key_value_pairs) > 0
    ):
        for key_value_pair in search_parameters.key_value_pairs:
            if ":" not in key_value_pair:
                raise HTTPException(status_code=400, detail="Invalid search key pair.")
            metadata_key_value_pairs.append(
                {
                    "metadata_key": key_value_pair.split(":")[0],
                    "metadata_value": key_value_pair.split(":")[1],
                }
            )
    else:
        metadata_key_value_pairs = None

    filtered_data_product_list = store.search_metadata(
        start_date=search_parameters.start_date,
        end_date=search_parameters.end_date,
        metadata_key_value_pairs=metadata_key_value_pairs,
    )
    return filtered_data_product_list


@app.get("/dataproductlist", response_class=Response)
async def data_products_list():
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    return json.dumps(store.metadata_list)


@app.post("/download")
async def download(file_object: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return download_file(file_object)


@app.post("/dataproductmetadata", response_class=Response)
async def data_product_metadata(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return store.load_metadata_file(file_object)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    DPD_API_Status.update_data_store_date_modified()
    store.ingest_metadata_files(file_object.fullPathName)
    logger.info("New data product metadata file loaded and store index updated")
    return "New data product metadata file loaded and store index updated"


@app.get("/layout")
async def layout():
    """API endpoint returns the columns that should be shown by default
    as well as their current width. In future I would like it to also
    return a user specific layout (possibly something the user has saved?)"""
    return DEFAULT_DISPLAY_LAYOUT
