"""API SDP Settings"""

import logging
import pathlib

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from ska_ser_logging import configure_logging
from starlette.config import Config

# pylint: disable=consider-using-from-import
import ska_sdp_dataproduct_api.api as api

configure_logging(level=uvicorn.config.LOGGING_CONFIG["loggers"]["uvicorn.error"]["level"])
logger = logging.getLogger(__name__)

config = Config(".env")
REINDEXING_DELAY = 300  # Only allow reindexing after 5 minutes
PERSISTENT_STORAGE_PATH: pathlib.Path = pathlib.Path(
    config("PERSISTENT_STORAGE_PATH", default="./tests/test_files/product"),
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
    default=(
        "./src/ska_sdp_dataproduct_api/components/elasticsearch/"
        "data_product_metadata_schema.json"
    ),
)

ES_HOST: str = config(
    "ES_HOST",
    default="http://localhost:9200",
)

VERSION: str = config(
    "SKA_SDP_DATAPRODUCT_API_VERSION",
    default=api.__version__,
)

STREAM_CHUNK_SIZE: int = int(
    config(
        "STREAM_CHUNK_SIZE",
        default=65536,
    )
)

# --PostgreSQL Configuration--
POSTGRESQL_HOST: str = config(
    "SDP_DATAPRODUCT_API_POSTGRESQL_HOST",
    default="localhost",
)

POSTGRESQL_PORT: int = int(
    config(
        "SDP_DATAPRODUCT_API_POSTGRESQL_PORT",
        default=5432,
    )
)

POSTGRESQL_USER: str = config(
    "SDP_DATAPRODUCT_API_POSTGRESQL_USER",
    default="",
)

POSTGRESQL_PASSWORD: str = config(
    "SDP_DATAPRODUCT_API_POSTGRESQL_PASSWORD",
    default="",
)
# ----

DATE_FORMAT: str = config("DATE_FORMAT", default="%Y-%m-%d")

API_ROOT_PATH: str = config("API_ROOT_PATH", default="")

app = FastAPI()

app = FastAPI(root_path=API_ROOT_PATH)

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
    {"name": "execution_block", "width": 250},
    {"name": "date_created", "width": 150},
    {"name": "observer", "width": 150},
    {"name": "processing_block", "width": 250},
    {"name": "Intent", "width": 300},
    {"name": "notes", "width": 500},
    {"name": "file_size", "width": 80},
    {"name": "status", "width": 80},
]
