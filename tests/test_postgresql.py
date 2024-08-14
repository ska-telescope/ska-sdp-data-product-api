"""Module to test PostgresConnector"""

import pytest

from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import (
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_TABLE_NAME,
    POSTGRESQL_USER,
)

# pylint: disable=duplicate-code


@pytest.fixture(autouse=True)
def clean_database(mocker):
    """Mock connection logic for setup and teardown"""
    inmemory_store_mocked = PostgresConnector(
        host=POSTGRESQL_HOST,
        port=POSTGRESQL_PORT,
        user=POSTGRESQL_USER,
        password=POSTGRESQL_PASSWORD,
        table_name=POSTGRESQL_TABLE_NAME,
    )
    mocker.patch.object(inmemory_store_mocked, "_connect")
    mocker.patch.object(inmemory_store_mocked, "conn")
    yield
