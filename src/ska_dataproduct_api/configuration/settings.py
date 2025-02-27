"""API Settings"""

import logging
import pathlib

import ska_ser_logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.config import Config

# pylint: disable=consider-using-from-import
import ska_dataproduct_api.api as api

config = Config(".env")

DEBUG: bool = config("API_VERBOSE", cast=bool, default=False)
LOGGING_LEVEL = logging.DEBUG if DEBUG else logging.WARNING
ska_ser_logging.configure_logging(LOGGING_LEVEL)
logger = logging.getLogger(__name__)
logger.info("Logging started for ska_dataproduct_api at level %s", LOGGING_LEVEL)


SECRETS_FILE_PATH: pathlib.Path = pathlib.Path(
    config("SKA_DATAPRODUCT_API_SECRETS_FILE_PATH", default=".secrets")
)

if not SECRETS_FILE_PATH.exists():
    SECRETS_FILE_PATH = None

secrets = Config(SECRETS_FILE_PATH)

REINDEXING_DELAY: int = int(
    config(
        "REINDEXING_DELAY",
        default=300,
    )
)

PERSISTENT_STORAGE_PATH: pathlib.Path = pathlib.Path(
    config("PERSISTENT_STORAGE_PATH", default="./tests/test_files/product"),
)
try:
    ABSOLUTE_PERSISTENT_STORAGE_PATH = PERSISTENT_STORAGE_PATH.resolve()
except Exception as exception:  # pylint: disable=broad-exception-caught
    logger.exception(
        "Could not resolve PERSISTENT_STORAGE_PATH: %s, %s", PERSISTENT_STORAGE_PATH, exception
    )
    ABSOLUTE_PERSISTENT_STORAGE_PATH = PERSISTENT_STORAGE_PATH

PVCNAME: str = config(
    "PVCNAME",
    default="None (using local test data)",
)

CONFIGURATION_FILES_PATH: pathlib.Path = pathlib.Path(__file__).parent

REACT_APP_SKA_DATAPRODUCT_DASHBOARD_URL: str = config(
    "REACT_APP_SKA_DATAPRODUCT_DASHBOARD_URL",
    default="http://localhost",
)

REACT_APP_SKA_DATAPRODUCT_DASHBOARD_PORT: str = config(
    "REACT_APP_SKA_DATAPRODUCT_DASHBOARD_PORT",
    default="8100",
)

METADATA_FILE_NAME: str = config(
    "METADATA_FILE_NAME",
    default="ska-data-product.yaml",
)

VERSION: str = config(
    "SKA_DATAPRODUCT_API_VERSION",
    default=api.__version__,
)

STREAM_CHUNK_SIZE: int = int(
    config(
        "STREAM_CHUNK_SIZE",
        default=65536,
    )
)

# ----
# PostgreSQL Variables
POSTGRESQL_HOST: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_HOST",
    default="",
)

POSTGRESQL_PORT: int = int(
    config(
        "SKA_DATAPRODUCT_API_POSTGRESQL_PORT",
        default=5432,
    )
)

POSTGRESQL_USER: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_USER",
    default="postgres",
)

POSTGRESQL_PASSWORD: str = secrets(
    "SKA_DATAPRODUCT_API_POSTGRESQL_PASSWORD",
    default="",
)

POSTGRESQL_DBNAME: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_DBNAME",
    default=("postgres"),
)

POSTGRESQL_SCHEMA: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_SCHEMA",
    default=("public"),
)

POSTGRESQL_METADATA_TABLE_NAME: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_METADATA_TABLE_NAME",
    default=("data_products_metadata_v2"),
)

POSTGRESQL_ANNOTATIONS_TABLE_NAME: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_ANNOTATIONS_TABLE_NAME",
    default=("data_products_annotations_v1"),
)

POSTGRESQL_QUERY_SIZE_LIMIT: int = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_QUERY_SIZE_LIMIT",
    default=(100),
)

DLM_INTERFACE_ENABLED: bool = config(
    "SKA_DATAPRODUCT_DLM_INTERFACE_ENABLED",
    default=(False),
)

POSTGRESQL_DLM_SCHEMA: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_DLM_SCHEMA",
    default=("dlm"),
)

POSTGRESQL_DLM_METADATA_TABLE_NAME: str = config(
    "SKA_DATAPRODUCT_API_POSTGRESQL_DLM_METADATA_TABLE_NAME",
    default=("data_item"),
)

# ----
# SKA Permissions API
SKA_PERMISSIONS_API_HOST: str = config(
    "SKA_PERMISSIONS_API_HOST",
    default="http://localhost:8000",
)

# ----
DATE_FORMAT: str = config("DATE_FORMAT", default="%Y-%m-%d")

API_ROOT_PATH: str = config("API_ROOT_PATH", default="")

app = FastAPI()

app = FastAPI(root_path=API_ROOT_PATH)


def origins() -> list:
    """Returns a list of unique origins.

    Leverages the built-in `set` data structure for efficient removal of duplicates.
    """

    known_origins = [
        "http://localhost",
        "http://localhost:8000",
        "http://localhost:" + REACT_APP_SKA_DATAPRODUCT_DASHBOARD_PORT,
        REACT_APP_SKA_DATAPRODUCT_DASHBOARD_URL,
        REACT_APP_SKA_DATAPRODUCT_DASHBOARD_URL + ":" + REACT_APP_SKA_DATAPRODUCT_DASHBOARD_PORT,
    ]

    return list(set(known_origins))


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins(),
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
