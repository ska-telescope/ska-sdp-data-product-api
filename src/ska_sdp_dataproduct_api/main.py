"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import json
import logging

from fastapi import BackgroundTasks
from fastapi import HTTPException, Response

from ska_sdp_dataproduct_api.core.helperfunctions import (
    DPDAPIStatus,
    FileUrl,
    SearchParametersClass,
    download_file,
    ingest_metadata_files,
    load_metadata_file,
)
from ska_sdp_dataproduct_api.core.settings import ES_HOST, app
from ska_sdp_dataproduct_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.inmemorystore.inmemorystore import (
    InMemoryDataproductIndex,
)

logger = logging.getLogger(__name__)

DPD_API_Status = DPDAPIStatus()

elasticsearch_metadata_store = ElasticsearchMetadataStore()
elasticsearch_metadata_store.connect(hosts=ES_HOST)

in_memory_metadata_store = InMemoryDataproductIndex(
    elasticsearch_metadata_store.es_search_enabled,
)


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return DPD_API_Status.status(
        elasticsearch_metadata_store.es_search_enabled
    )


@app.get("/reindexdataproducts")
async def reindex_data_products(background_tasks: BackgroundTasks):
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    DPD_API_Status.update_data_store_date_modified()
    if elasticsearch_metadata_store.es_search_enabled:
        store = elasticsearch_metadata_store
    else:
        store = in_memory_metadata_store
    background_tasks.add_task(store.reindex)
    logger.info("Metadata store cleared and re-indexed")
    return "Metadata store cleared and re-indexed"


@app.post("/dataproductsearch", response_class=Response)
async def data_products_search(search_parameters: SearchParametersClass):
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    if not elasticsearch_metadata_store.es_search_enabled:
        elasticsearch_metadata_store.connect(hosts=ES_HOST)
        if not elasticsearch_metadata_store.es_search_enabled:
            raise HTTPException(
                status_code=503, detail="Elasticsearch not found"
            )
    filtered_data_product_list = elasticsearch_metadata_store.search_metadata(
        start_date=search_parameters.start_date,
        end_date=search_parameters.end_date,
        metadata_key=search_parameters.key_pair.split(":")[0],
        metadata_value=search_parameters.key_pair.split(":")[1],
    )
    return filtered_data_product_list


@app.get("/dataproductlist", response_class=Response)
async def data_products_list():
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    return json.dumps(in_memory_metadata_store.metadata_list)


@app.post("/download")
async def download(file_object: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return download_file(file_object)


@app.post("/dataproductmetadata", response_class=Response)
async def data_product_metadata(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return load_metadata_file(file_object)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    DPD_API_Status.update_data_store_date_modified()
    if elasticsearch_metadata_store.es_search_enabled:
        ingest_metadata_files(
            elasticsearch_metadata_store, file_object.fullPathName
        )
    else:
        ingest_metadata_files(
            in_memory_metadata_store, file_object.fullPathName
        )
    logger.info(
        "New data product metadata file loaded and store index updated"
    )
    return "New data product metadata file loaded and store index updated"
