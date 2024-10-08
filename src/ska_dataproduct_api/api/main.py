"""This API exposes SKA Data Products to the SKA Data Product Dashboard."""

import logging

from fastapi import BackgroundTasks, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

from ska_dataproduct_api.components.authorisation.authorisation import (
    extract_token,
    get_user_groups,
)
from ska_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_dataproduct_api.components.store.store_factory import (
    select_metadata_store_class,
    select_search_store_class,
)
from ska_dataproduct_api.configuration.settings import (
    ABSOLUTE_PERSISTENT_STORAGE_PATH,
    DEFAULT_DISPLAY_LAYOUT,
    METADATA_FILE_NAME,
    app,
)
from ska_dataproduct_api.utilities.helperfunctions import (
    DPDAPIStatus,
    ExecutionBlock,
    FilePaths,
    SearchParametersClass,
    download_file,
)

logger = logging.getLogger(__name__)

metadata_store = select_metadata_store_class()

search_store = select_search_store_class(metadata_store)

DPD_API_Status = DPDAPIStatus(
    search_store_status=search_store.status,
    metadata_store_status=metadata_store.status,
)


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return DPD_API_Status.status()


@app.get("/reindexdataproducts", status_code=202)
async def reindex_data_products(background_tasks: BackgroundTasks):
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    background_tasks.add_task(metadata_store.reindex_persistent_volume)
    logger.info("Metadata search_store re-indexed")
    return "Metadata is set to be re-indexed"


@app.post("/dataproductsearch")
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
                    "keyPair": key_value_pair.split(":")[0],
                    "valuePair": key_value_pair.split(":")[1],
                }
            )

    search_options = {
        "items": [
            {
                "field": "date_created",
                "operator": "greaterThan",
                "value": search_parameters.start_date,
            },
            {"field": "date_created", "operator": "lessThan", "value": search_parameters.end_date},
            {"field": "formFields", "keyPairs": metadata_key_value_pairs},
        ],
        "logicOperator": "and",
    }
    filtered_data = search_store.filter_data(
        mui_data_grid_filter_model={},
        search_panel_options=search_options,
        users_user_group_list=[],
    )
    return filtered_data


@app.post("/filterdataproducts")
@extract_token
async def filter_data(token: str, request: Request) -> list:
    """
    Filters product data based on provided criteria.

    This endpoint receives a JSON object containing filter parameters in the request body.
    It applies filters to the in-memory data search_store.

    Args:
        token: The validated access token.
        request: The incoming request object.

    Returns:
        list: A list of filtered product data objects.
    """
    users_group_assignments = await get_user_groups(token=token)
    users_user_group_list = users_group_assignments["user_groups"]

    # Access the request body
    body = await request.json()
    mui_data_grid_filter_model = body.get("filterModel", {})
    search_panel_options = body.get("searchPanelOptions", {})

    # Rest of your code using body and request object
    filtered_data = search_store.filter_data(
        mui_data_grid_filter_model=mui_data_grid_filter_model,
        search_panel_options=search_panel_options,
        users_user_group_list=users_user_group_list,
    )

    return filtered_data


@app.get("/muidatagridconfig")
async def get_muidatagridconfig() -> dict:
    """
    Retrieves the MUI DataGrid configuration.

    This endpoint returns the configuration object used by the MUI DataGrid component,
    providing information about columns, sorting, filtering, and other aspects of the grid.

    Returns:
        dict: The MUI DataGrid configuration object.
    """
    return muiDataGridInstance.table_config


@app.post("/download", response_class=StreamingResponse)
async def download(data: ExecutionBlock):
    """This API endpoint returns a FileResponse that is used by a
    frontend to download a file"""
    if not data.execution_block:
        raise HTTPException(status_code=400, detail="Missing execution_block field in request")
    return download_file(metadata_store.get_data_product_file_path(data.execution_block))


@app.post("/dataproductmetadata")
async def data_product_metadata(data: ExecutionBlock):
    """This API endpoint returns the data products metadata in json format of a specified data
    product."""
    if not data.execution_block:
        raise HTTPException(status_code=400, detail="Missing execution_block field in request")
    return metadata_store.get_metadata(data.execution_block)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(
    file_object: FilePaths,
):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    metadata_store.ingest_file(
        ABSOLUTE_PERSISTENT_STORAGE_PATH / file_object.execution_block / METADATA_FILE_NAME
    )
    metadata_store.update_data_store_date_modified()
    logger.info("New data product metadata file loaded and search_store index updated")
    return "New data product metadata file loaded and search_store index updated"


@app.post("/ingestnewmetadata")
async def ingest_new_metadata(
    metadata: dict,
):
    """This API endpoint takes JSON data product metadata and ingests into
    the appropriate search_store."""
    metadata_store.ingest_metadata(metadata)
    search_store.insert_metadata_in_search_store(metadata)
    metadata_store.update_data_store_date_modified()
    logger.info("New data product metadata received and search_store index updated")
    return "New data product metadata received and search store index updated"


@app.get("/layout")
async def layout():
    """API endpoint returns the columns that should be shown by default
    as well as their current width. In future I would like it to also
    return a user specific layout (possibly something the user has saved?)"""
    return DEFAULT_DISPLAY_LAYOUT
