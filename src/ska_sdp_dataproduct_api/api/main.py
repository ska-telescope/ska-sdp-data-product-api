"""This API exposes SDP Data Products to the SDP Data Product Dashboard."""

import json
import logging
from typing import Dict, List, Optional

from fastapi import BackgroundTasks, Body, Response
from fastapi.exceptions import HTTPException

from ska_sdp_dataproduct_api.components.metadatastore.store_factory import (
    select_correct_store_class,
)
from ska_sdp_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_sdp_dataproduct_api.components.postgresql.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import DEFAULT_DISPLAY_LAYOUT, app
from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    DataProductMetaData,
    DPDAPIStatus,
    FileUrl,
    SearchParametersClass,
    download_file,
)

logger = logging.getLogger(__name__)

search_store = select_correct_store_class()
postgresql_connector = PostgresConnector()
DPD_API_Status = DPDAPIStatus(
    search_store=search_store.status, postgresql_status=postgresql_connector.status
)


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return DPD_API_Status.status(search_store.es_search_enabled)


@app.get("/reindexdataproducts", status_code=202)
async def reindex_data_products(background_tasks: BackgroundTasks):
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    background_tasks.add_task(search_store.reindex)
    logger.info("Metadata search_store cleared and re-indexed")
    return "Metadata is set to be cleared and re-indexed"


@app.post("/dataproductsearch", response_class=Response)
async def data_products_search(search_parameters: SearchParametersClass):
    """This API endpoint returns a list of all the data products
    in the PERSISTENT_STORAGE_PATH
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

    filtered_data_product_list = search_store.search_metadata(
        start_date=search_parameters.start_date,
        end_date=search_parameters.end_date,
        metadata_key_value_pairs=metadata_key_value_pairs,
    )
    return filtered_data_product_list


@app.post("/filterdataproducts")
async def filter_data(body: Optional[Dict] = Body(...)) -> List:
    """
    Filters product data based on provided criteria.

    This endpoint receives a JSON object containing filter parameters in the request body.
    It applies filters to the in-memory data search_store.

    Args:
        filter_data (Optional[List]): The filter criteria.
            Defaults to None.

    Returns:
        List: A list of filtered product data objects.
    """
    muiDataGridInstance.load_inmemory_store_data(search_store)
    mui_data_grid_filter_model = body.get("filterModel", {})
    search_panel_options = body.get("searchPanelOptions", {})

    mui_filtered_data = search_store.apply_filters(
        muiDataGridInstance.rows.copy(), mui_data_grid_filter_model
    )
    searchbox_filtered_data = search_store.apply_filters(mui_filtered_data, search_panel_options)

    return searchbox_filtered_data


@app.get("/muidatagridconfig")
async def get_muidatagridconfig() -> Dict:
    """
    Retrieves the MUI DataGrid configuration.

    This endpoint returns the configuration object used by the MUI DataGrid component,
    providing information about columns, sorting, filtering, and other aspects of the grid.

    Returns:
        Dict: The MUI DataGrid configuration object.
    """

    return muiDataGridInstance.table_config


@app.get("/dataproductlist", response_class=Response)
async def data_products_list():
    """This API endpoint returns a list of all the data products
    in the PERSISTENT_STORAGE_PATH
    """
    return json.dumps(search_store.metadata_list)


@app.post("/download")
async def download(file_object: FileUrl):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    return download_file(file_object)


@app.post("/dataproductmetadata", response_class=Response)
async def data_product_metadata(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    return search_store.load_metadata(file_object)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(file_object: FileUrl):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    search_store.update_data_store_date_modified()
    search_store.ingest_metadata_files(file_object.fullPathName)
    logger.info("New data product metadata file loaded and search_store index updated")
    return "New data product metadata file loaded and search_store index updated"


@app.post("/ingestnewmetadata")
async def ingest_new_metadata(metadata: DataProductMetaData):
    """This API endpoint takes JSON data product metadata and ingests into
    the appropriate search_store."""
    search_store.update_data_store_date_modified()
    search_store.ingest_metadata_object(metadata)
    logger.info("New data product metadata received and search_store index updated")
    return "New data product metadata received and search store index updated"


@app.get("/layout")
async def layout():
    """API endpoint returns the columns that should be shown by default
    as well as their current width. In future I would like it to also
    return a user specific layout (possibly something the user has saved?)"""
    return DEFAULT_DISPLAY_LAYOUT
