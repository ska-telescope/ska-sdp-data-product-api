"""API SDP Settings"""

import pathlib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.config import Config

import ska_sdp_dataproduct_api

config = Config(".env")
PERSISTANT_STORAGE_PATH: pathlib.Path = pathlib.Path(
    config("PERSISTANT_STORAGE_PATH", default="./tests/test_files"),
)
REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL: str = config(
    "REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL",
    default="http://localhost",
)
REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT: str = config(
    "REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT",
    default="8100",
)

METADATA_FILE_NAME: str = config(
    "METADATA_FILE_NAME",
    default="ska-data-product.yaml",
)

METADATA_ES_SCHEMA_FILE: str = config(
    "METADATA_ES_SCHEMA_FILE",
    default="./elasticsearch/data_product_metadata_schema.json",
)

ES_HOST: str = config(
    "ES_HOST",
    default="http://localhost:9200",
)

VERSION: str = config(
    "SKA_SDP_DATAPRODUCT_API_VERSION",
    default=ska_sdp_dataproduct_api.__version__,
)

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost" + ":" + REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT,
    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL,
    REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_URL
    + ":"
    + REACT_APP_SKA_SDP_DATAPRODUCT_DASHBOARD_PORT,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
