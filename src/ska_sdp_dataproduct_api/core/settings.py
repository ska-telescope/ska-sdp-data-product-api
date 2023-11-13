"""API SDP Settings"""

import logging
import pathlib

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from ska_ser_logging import configure_logging
from starlette.config import Config

import ska_sdp_dataproduct_api

configure_logging(
    level=uvicorn.config.LOGGING_CONFIG["loggers"]["uvicorn.error"]["level"]
)
logger = logging.getLogger(__name__)

config = Config(".env")
REINDEXING_DELAY = 300  # Only allow reindexing after 5 minutes
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

API_URL_SUBDIRECTORY: str = config("API_URL_SUBDIRECTORY", default="")
STREAM_CHUNK_SIZE: int = int(
    config(
        "STREAM_CHUNK_SIZE",
        default=65536,
    )
)

app = FastAPI()

app = FastAPI(root_path=API_URL_SUBDIRECTORY)

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


DEFAULT_DISPLAY_LAYOUT = [
    {"name": "execution_block", "width": 180},
    {"name": "id", "width": 20},
    {"name": "date_created", "width": 60},
    {"name": "observer", "width": 60},
    {"name": "execution_block", "width": 120},
    {"name": "processing_block", "width": 180},
    {"name": "Intent", "width": 180},
    {"name": "notes", "width": 180},
]
