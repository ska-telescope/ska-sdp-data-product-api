"""Module to test PostgresConnector"""

import pathlib

import pytest

from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector

# pylint: disable=redefined-outer-name


@pytest.fixture
def mocked_postgres_connector(mocker):
    """
    Provides a mocked instance of PostgresConnector for testing.
    """

    # Mock the _connect method to prevent actual connection attempt
    mocker.patch.object(PostgresConnector, "_connect")
    mocker.patch.object(PostgresConnector, "get_data_by_execution_block")

    # Create the instance with desired arguments (replace with yours)
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

    yield connector


def test_status(mocked_postgres_connector):
    """Mock connection logic for setup and teardown"""
    status = mocked_postgres_connector.status()
    expected_status = {
        "store_type": "Persistent PosgreSQL metadata store",
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "running": True,
        "dbname": "test_db",
        "schema": "public",
        "table_name": "my_table",
        "number_of_dataproducts": 0,
        "postgresql_version": "",
    }

    assert status == expected_status


def test_build_connection_string(mocked_postgres_connector):
    """
    Tests that the _build_connection_string function constructs the connection string correctly.
    """
    # Call the function under test
    connection_string = mocked_postgres_connector.build_connection_string()

    # Assert the constructed connection string
    assert connection_string == (
        "dbname='test_db' user='test_user' password='test_password' host='localhost' port='5432' \
options='-c search_path=\"public\"'"
    )


def test_get_data_product_file_path_success(mocked_postgres_connector):
    """Tests successful retrieval of file path."""
    execution_block = "test_block"
    expected_file_path = pathlib.Path("/path/to/file")
    mocked_postgres_connector.get_data_by_execution_block.return_value = {
        "dataproduct_file": str(expected_file_path)
    }

    result = mocked_postgres_connector.get_data_product_file_path(execution_block)

    assert result == expected_file_path
    mocked_postgres_connector.get_data_by_execution_block.assert_called_once_with(execution_block)


def test_get_data_product_file_path_not_found(mocked_postgres_connector):
    """Tests when file path is not found."""
    execution_block = "test_block"
    mocked_postgres_connector.get_data_by_execution_block.return_value = None

    result = mocked_postgres_connector.get_data_product_file_path(execution_block)

    assert result == {}
    mocked_postgres_connector.get_data_by_execution_block.assert_called_once_with(execution_block)


def test_get_data_product_file_path_key_error(mocked_postgres_connector, caplog):
    """Tests handling of KeyError."""
    execution_block = "test_block"
    mocked_postgres_connector.get_data_by_execution_block.side_effect = KeyError()

    result = mocked_postgres_connector.get_data_product_file_path(execution_block)

    assert result == {}
    mocked_postgres_connector.get_data_by_execution_block.assert_called_once_with(execution_block)
    assert "File path not found for execution block" in caplog.text
