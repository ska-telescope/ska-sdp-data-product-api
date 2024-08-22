"""Module to test PostgresConnector"""

import pytest

from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import (
    POSTGRESQL_DBNAME,
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_SCHEMA,
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
        dbname=POSTGRESQL_DBNAME,
        schema=POSTGRESQL_SCHEMA,
        table_name=POSTGRESQL_TABLE_NAME,
    )
    mocker.patch.object(inmemory_store_mocked, "_connect")
    mocker.patch.object(inmemory_store_mocked, "conn")
    yield


def test_status(mocker):
    """Mock connection logic for setup and teardown"""
    inmemory_store_mocked = PostgresConnector(
        host=POSTGRESQL_HOST,
        port=POSTGRESQL_PORT,
        user=POSTGRESQL_USER,
        password=POSTGRESQL_PASSWORD,
        dbname=POSTGRESQL_DBNAME,
        schema=POSTGRESQL_SCHEMA,
        table_name=POSTGRESQL_TABLE_NAME,
    )
    mocker.patch.object(inmemory_store_mocked, "_connect")
    mocker.patch.object(inmemory_store_mocked, "conn")

    status = inmemory_store_mocked.status()
    expected_status = {
        "store_type": "Persistent PosgreSQL metadata store",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "running": False,
        "dbname": "postgres",
        "schema": "sdp_sdp_dataproduct_dashboard_dev",
        "table_name": "data_products_metadata_v1",
        "number_of_dataproducts": 0,
        "postgresql_version": "",
    }

    assert status == expected_status


def test_build_connection_string(mocker):
    """
    Tests that the _build_connection_string function constructs the connection string correctly.
    """
    inmemory_store_mocked = PostgresConnector(
        host=POSTGRESQL_HOST,
        port=POSTGRESQL_PORT,
        user=POSTGRESQL_USER,
        password=POSTGRESQL_PASSWORD,
        dbname=POSTGRESQL_DBNAME,
        schema=POSTGRESQL_SCHEMA,
        table_name=POSTGRESQL_TABLE_NAME,
    )
    mocker.patch.object(inmemory_store_mocked, "_connect")
    mocker.patch.object(inmemory_store_mocked, "conn")

    # Call the function under test
    connection_string = inmemory_store_mocked.build_connection_string()

    # Assert the constructed connection string
    assert connection_string == (
        "dbname='postgres' user='postgres' password='password' host='localhost' port='5432' \
options='-c search_path=\"sdp_sdp_dataproduct_dashboard_dev\"'"
    )
