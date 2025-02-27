"""This API exposes SKA Data Products to the SKA Data Product Dashboard."""

import json
import logging
from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

from fastapi import BackgroundTasks, Request, Response, status
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation
from ska_dataproduct_api.components.authorisation.authorisation import (
    extract_token,
    get_user_groups,
    get_user_profile,
)
from ska_dataproduct_api.components.muidatagrid.mui_datagrid import mui_data_grid_config_instance
from ska_dataproduct_api.components.pv_interface.pv_interface import PVInterface
from ska_dataproduct_api.components.store.persistent.postgresql import PGMetadataStore
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
    validate_data_product_identifier,
)

logger = logging.getLogger(__name__)

pv_interface = PVInterface()


@app.on_event("startup")
async def startup_event():
    """This function will execute a background tasks to reindex of the data product when the
    application starts."""
    background_tasks = BackgroundTasks()
    background_tasks.add_task(reindex_data_products_stores())


metadata_store = select_metadata_store_class()

search_store = select_search_store_class(metadata_store)

DPD_API_Status = DPDAPIStatus(
    pv_interface_status=pv_interface.status,
    search_store_status=search_store.status,
    metadata_store_status=metadata_store.status,
)


def reindex_data_products_stores() -> None:
    """Background tasks to reindex the data products on the persistent volume"""
    try:
        pv_interface.index_all_data_product_files_on_pv()
        metadata_store.reload_all_data_products_in_index(pv_index=pv_interface.pv_index)
        logger.info("Persistent volume re-indexed and stores updated.")
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

    body = await request.json()
    mui_data_grid_filter_model = body.get("filterModel", {})
    search_panel_options = body.get("searchPanelOptions", {})

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
    return mui_data_grid_config_instance.table_config


@app.post("/download", response_class=StreamingResponse)
async def download(data_product_identifier: DataProductIdentifier) -> StreamingResponse:
    """
    Downloads a file based on the provided DataProductIdentifier.

    Args:
        data_product_identifier: The identifier containing either a UUID or ExecutionBlock.

    Returns:
        A StreamingResponse containing the file data.

    Raises:
        HTTPException:
            - 400: If neither UUID nor ExecutionBlock is provided.
            - 403: Permission denied.
            - 404: If the file is not found or access is denied.
            - 500: If an unexpected error occurs during file retrieval.
    """
    try:
        validate_data_product_identifier(data_product_identifier)
        file_path_list = metadata_store.get_data_product_file_paths(data_product_identifier)
        return download_file(file_path_list)
    except (ValueError, TypeError) as error:
        raise HTTPException(
            status_code=400, detail=f"Invalid product identifier: {error}"
        ) from error
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=f"Permission denied: {error}") from error
    except (FileNotFoundError, KeyError) as error:
        raise HTTPException(status_code=404, detail=f"File not found: {error}") from error
    except Exception as error:
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {error}"
        ) from error


@app.post("/dataproductmetadata")
async def data_product_metadata(data_product_identifier: DataProductIdentifier) -> dict:
    """
    This API endpoint retrieves and returns the data product metadata in JSON format
    for a specified data product identified by its UUID, or {} if no metadata is found.

    Raises:
        HTTPException: 400 Bad Request if the request body is missing the "uid" field.
    """
    try:
        validate_data_product_identifier(data_product_identifier)
    except (ValueError, TypeError) as error:
        raise HTTPException(
            status_code=400, detail=f"Invalid product identifier: {error}"
        ) from error

    return metadata_store.get_metadata(data_product_identifier)


@app.post("/ingestnewdataproduct")
async def ingest_new_data_product(
    file_object: FilePaths,
):
    """This API endpoint returns the data products metadata in json format of
    a specified data product."""
    try:
        data_product_uid = metadata_store.ingest_file(
            ABSOLUTE_PERSISTENT_STORAGE_PATH / file_object.execution_block / METADATA_FILE_NAME
        )
        metadata_store.date_modified = datetime.now(tz=timezone.utc)
        return {
            "status": "success",
            "message": "New data product received and search store index updated",
            "uid": data_product_uid,
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
        data_product_uid = metadata_store.ingest_metadata(
            metadata_file_dict=metadata, data_store="dpd"
        )
        metadata_store.date_modified = datetime.now(tz=timezone.utc)
        logger.info("New data product metadata received and saved in the DPD datastore.")
        return {
            "status": "success",
            "message": "New data product metadata received and saved in the DPD datastore.",
            "uid": data_product_uid,
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
@extract_token
async def annotation(token: str, request: Request):
    """API endpoint to create new annotations linked to a data product."""

    users_profile = await get_user_profile(token=token)
    data = await request.json()

    data_product_annotation = DataProductAnnotation(
        annotation_text=data.get("annotation_text"),
        annotation_id=data.get("annotation_id", None),
        user_principal_name=users_profile["user_profile"]["userPrincipalName"],
        data_product_uid=data.get("data_product_uid"),
    )

    if not isinstance(metadata_store, (PGMetadataStore, MagicMock)):
        logger.info("PostgresSQL not available, cannot access data annotations.")
        return Response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=json.dumps(
                {
                    "status": "Received but not processed",
                    "message": "PostgresSQL is not available, cannot access data annotations.",
                }
            ),
            media_type="application/json",
        )
    try:
        metadata_store.save_annotation(data_product_annotation)
        if data_product_annotation.annotation_id is None:
            logger.info("New annotation created successfully.")
            return Response(
                status_code=status.HTTP_201_CREATED,
                content=json.dumps(
                    {
                        "status": "success",
                        "message": "New Data Annotation received and successfully saved.",
                    }
                ),
                media_type="application/json",
            )
        logger.info("Annotation updated successfully.")
        return Response(
            status_code=status.HTTP_200_OK,
            content=json.dumps(
                {
                    "status": "success",
                    "message": "Data Annotation received and updated successfully.",
                }
            ),
            media_type="application/json",
        )
    except Exception as exception:
        logger.exception("Error saving annotation: %s", exception)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error while saving new annotation. Error: {exception}",
        ) from exception


@app.get(
    "/annotations/{data_product_uid}", response_model=list[DataProductAnnotation] | list | dict
)
async def get_annotation_by_uid(
    data_product_uid: str, response: Response
) -> List[DataProductAnnotation] | list:
    """API GET endpoint to retrieve all annotations linked to a data product."""
    if not isinstance(metadata_store, (PGMetadataStore, MagicMock)):
        logger.info("PostgresSQL not available, cannot access data annotations.")
        response.status_code = status.HTTP_202_ACCEPTED
        return {
            "status": "Received but not processed",
            "message": "PostgresSQL is not available, cannot access data annotations.",
        }
    try:
        data_product_annotations = metadata_store.retrieve_annotations_by_uid(data_product_uid)
        if len(data_product_annotations) == 0:
            response.status_code = status.HTTP_204_NO_CONTENT
            return []
        return data_product_annotations
    except Exception as error:
        logger.error("Error retrieving annotations: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error while retrieving annotations. Error: {error}",
        ) from error
