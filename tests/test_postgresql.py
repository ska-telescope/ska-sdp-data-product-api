"""Module to test PostgresConnector"""

import pathlib
from unittest.mock import MagicMock, patch

import pytest

from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector

# pylint: disable=redefined-outer-name


@pytest.fixture
def mocked_postgres_connector():
    """
    Provides a mocked instance of PostgresConnector for testing.
    """

    with patch("psycopg.connect") as mock_connect:
        connector = PostgresConnector(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            dbname="test_db",
            schema="public",
            table_name="my_table",
        )

        # Set any additional properties you want to control
        connector.postgresql_running = True
        connector.postgresql_version = "mocked"
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        yield {"connector": connector, "cursor": mock_cursor}


def test_status(mocked_postgres_connector):
    """Mock connection logic for setup and teardown"""
    status = mocked_postgres_connector["connector"].status()
    expected_status = {
        "store_type": "Persistent PosgreSQL metadata store",
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "running": True,
        "dbname": "test_db",
        "schema": "public",
        "table_name": "my_table",
        "number_of_dataproducts": 1,
        "postgresql_version": "mocked",
    }

    assert status == expected_status


def test_build_connection_string(mocked_postgres_connector):
    """
    Tests that the _build_connection_string function constructs the connection string correctly.
    """
    # Call the function under test
    connection_string = mocked_postgres_connector["connector"].build_connection_string()

    # Assert the constructed connection string
    assert connection_string == (
        "dbname='test_db' user='test_user' password='test_password' host='localhost' port='5432' \
options='-c search_path=\"public\"'"
    )


def test_get_data_product_file_path_success(mocked_postgres_connector):
    """Tests successful retrieval of file path."""
    execution_block = "test_block"
    expected_file_path = pathlib.Path("tests/test_files/product/eb-m002-20221212-12345")

    mocked_postgres_connector["cursor"].fetchone.return_value = (
        {
            "dataproduct_file": "tests/test_files/product/eb-m002-20221212-12345",
        },
    )

    result = mocked_postgres_connector["connector"].get_data_product_file_path(execution_block)

    assert result == expected_file_path


def test_get_data_product_file_path_not_found(mocked_postgres_connector):
    """Tests when file path is not found."""
    execution_block = "test_block"

    mocked_postgres_connector["cursor"].fetchone.return_value = ({},)

    result = mocked_postgres_connector["connector"].get_data_product_file_path(execution_block)

    assert result == {}
