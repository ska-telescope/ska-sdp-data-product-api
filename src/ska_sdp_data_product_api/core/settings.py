"""API SDP Settings"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.config import Config

config = Config(".env")
PERSISTANT_STORAGE_PATH: str = config(
    "PERSISTANT_STORAGE_PATH",
    default="./tests/test_files",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL",
    default="http://localhost",
)
REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT: str = config(
    "REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT",
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

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost" + ":" + REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT,
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL,
    REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_URL
    + ":"
    + REACT_APP_SKA_SDP_DATA_PRODUCT_DASHBOARD_PORT,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
