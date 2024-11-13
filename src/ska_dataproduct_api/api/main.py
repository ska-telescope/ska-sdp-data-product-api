"""This API exposes SKA Data Products to the SKA Data Product Dashboard."""

import logging

from fastapi import BackgroundTasks, Request, status
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
    DataProductIdentifier,
    DPDAPIStatus,
    FilePaths,
    SearchParametersClass,
    download_file,
)
from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation

logger = logging.getLogger(__name__)

metadata_store = select_metadata_store_class()

search_store = select_search_store_class(metadata_store)

DPD_API_Status = DPDAPIStatus(
    search_store_status=search_store.status,
    metadata_store_status=metadata_store.status,
)


def reindex_data_products_stores() -> None:
    """Background tasks to reindex the data products on the persistent volume"""
    try:
        metadata_store.reindex_persistent_volume()
        search_store.load_metadata_from_store()
        logger.info("Metadata re-indexed")
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.exception("Metadata re-index failed: %s", exception)


@app.get("/status")
async def root():
    """An enpoint that just returns confirmation that the
    application is running"""
    return DPD_API_Status.status()


@app.get("/reindexdataproducts", status_code=202)
async def reindex_data_products(background_tasks: BackgroundTasks):
    """This endpoint clears the list of data products from memory and
    re-ingest the metadata of all data products found"""
    background_tasks.add_task(reindex_data_products_stores)
    return "Metadata re-index request has been added to the background tasks"


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
async def download(data_product_identifier: DataProductIdentifier) -> StreamingResponse:
    """
    Downloads a file based on the provided UUID or ExecutionBlock information.

    Raises:
        HTTPException: If the required data is missing or there's an error
                       retrieving or accessing the file.
    """

    if not data_product_identifier.uuid and not data_product_identifier.execution_block:
        raise HTTPException(status_code=400, detail="Missing UUID or ExecutionBlock")

    try:
        file_path_list = metadata_store.get_data_product_file_paths(data_product_identifier)
        return download_file(file_path_list)
    except (FileNotFoundError, PermissionError) as error:
        raise HTTPException(status_code=404, detail=f"Failed to access file: {error}") from error


@app.post("/dataproductmetadata")
async def data_product_metadata(data_product_identifier: DataProductIdentifier) -> dict:
    """
    This API endpoint retrieves and returns the data product metadata in JSON format
    for a specified data product identified by its UUID, or {} if no metadata is found.

    Raises:
        HTTPException: 400 Bad Request if the request body is missing the "uuid" field.
    """
    if not data_product_identifier.uuid:
        raise HTTPException(status_code=400, detail="Missing uuid field in request")

    return metadata_store.get_metadata(data_product_identifier.uuid)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(
    file_object: FilePaths,
):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    try:
        data_product_uuid = metadata_store.ingest_file(
            ABSOLUTE_PERSISTENT_STORAGE_PATH / file_object.execution_block / METADATA_FILE_NAME
        )
        metadata_store.update_data_store_date_modified()
        return {
            "status": "success",
            "message": "New data product received and search store index updated",
            "uuid": data_product_uuid,
        }, status.HTTP_201_CREATED
    except Exception as error:
        logger.error("Error ingesting metadata: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during metadata ingestion. Error: {error}",
        ) from error


@app.post("/ingestnewmetadata")
async def ingest_new_metadata(
    metadata: dict,
):
    """
    This API endpoint ingests new data product metadata in JSON format and
    updates the search store index. Raises a 400 Bad Request exception
    if the provided metadata is not a valid dictionary.

    Args:
        metadata (dict[str, str]): The data product metadata in JSON format.

    Raises:
        HTTPException: Raised if the provided metadata is not a valid dictionary.

    Returns:
        dict: A JSON response containing a success message and the ingested metadata.
    """

    if not isinstance(metadata, dict):
        raise HTTPException(
            status_code=400, detail="Invalid metadata format. Must be a dictionary."
        )

    try:
        data_product_uuid = metadata_store.ingest_metadata(metadata)
        search_store.insert_metadata_in_search_store(metadata)
        metadata_store.update_data_store_date_modified()
        logger.info("New data product metadata received and search_store index updated")
        return {
            "status": "success",
            "message": "New data product metadata received and search store index updated",
            "uuid": data_product_uuid,
        }, status.HTTP_201_CREATED

    except Exception as error:
        logger.error("Error ingesting metadata: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during metadata ingestion. Error: {error}",
        ) from error


@app.get("/layout")
async def layout():
    """API endpoint returns the columns that should be shown by default
    as well as their current width. In future I would like it to also
    return a user specific layout (possibly something the user has saved?)"""
    return DEFAULT_DISPLAY_LAYOUT


@app.post("/annotation")
async def annotation(data_product_annotation: DataProductAnnotation):
    """API endpoint to create new annotations linked to a data product."""
    metadata_store.insert_annotation(data_product_annotation)