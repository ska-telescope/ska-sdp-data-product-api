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

CONFIGURATION_FILES_PATH: pathlib.Path = pathlib.Path(__file__).parent

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

# ElasticSearch Variables
ELASTICSEARCH_HOST: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_HOST",
    default="https://localhost",
)

ELASTICSEARCH_PORT: int = int(
    config(
        "SDP_DATAPRODUCT_API_ELASTIC_PORT",
        default=9200,
    )
)

ELASTICSEARCH_HTTP_CA: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_HTTP_CA",
    default=None,
)

ELASTICSEARCH_USER: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_USER",
    default="elastic",
)

ELASTICSEARCH_PASSWORD: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_PASSWORD",
    default="",
)

ELASTICSEARCH_METADATA_SCHEMA_FILE: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_METADATA_SCHEMA_FILE",
    default=(
        "./src/ska_sdp_dataproduct_api/components/elasticsearch/"
        "data_product_metadata_schema.json"
    ),
)

ELASTICSEARCH_INDICES: str = config(
    "SDP_DATAPRODUCT_API_ELASTIC_INDICES",
    default=("localhost-sdp-dataproduct-dashboard-dev"),
)
# ----
# PostgreSQL Variables
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
    default="postgres",
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
