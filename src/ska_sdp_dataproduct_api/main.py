"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import json

from fastapi import HTTPException, Response

from ska_sdp_dataproduct_api.core.helperfunctions import (
    DataProductMetaData,
    FileUrl,
    SearchParametersClass,
    downloadfile,
    ingestjson,
    ingestmetadatafiles,
    loadmetadatafile,
)
from ska_sdp_dataproduct_api.core.settings import ES_HOST, app
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
async def reindex_data_products():
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    if elk_metadata_store.es_search_enabled:
        elk_metadata_store.reindex()
    else:
        in_memory_metadata_store.reindex()
    return "Metadata store cleared and re-indexed"


@app.post("/dataproductsearch", response_class=Response)
async def data_products_search(search_parameters: SearchParametersClass):
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
async def data_products_list():
    """This API endpoint returns a list of all the data products
    in the PERSISTANT_STORAGE_PATH
    """
    return json.dumps(in_memory_metadata_store.metadata_list)


@app.post("/download")
async def download(file_object: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return downloadfile(file_object)


@app.post("/dataproductmetadata", response_class=Response)
async def data_product_metadata(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return loadmetadatafile(file_object)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    if elk_metadata_store.es_search_enabled:
        ingestmetadatafiles(elk_metadata_store, file_object.fullPathName)
    else:
        ingestmetadatafiles(in_memory_metadata_store, file_object.fullPathName)
    return "Data product metadata file loaded and store index updated"


@app.post("/ingestjson")
async def ingest_json(dataproduct: DataProductMetaData):
    if elk_metadata_store.es_search_enabled:
        ingestjson(elk_metadata_store, dataproduct)
    else:
        ingestjson(in_memory_metadata_store, dataproduct)
