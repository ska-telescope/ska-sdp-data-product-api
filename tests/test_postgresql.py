"""Module to test PostgresConnector"""

import pytest

from ska_sdp_dataproduct_api.components.postgresql.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import POSTGRESQL_TABLE_NAME

# pylint: disable=duplicate-code


@pytest.fixture(autouse=True)
def clean_database(mocker):
    """Mock connection logic for setup and teardown"""
    inmemory_store_mocked = PostgresConnector()
    mocker.patch.object(inmemory_store_mocked, "connect")
    mocker.patch.object(inmemory_store_mocked, "conn")
    yield


def test_create_metadata_table(mocker):
    """Test for create_metadata_table"""
    # Mock cursor execution to avoid actual database interaction
    inmemory_store_mocked = PostgresConnector()

    mock_cursor = mocker.Mock()
    mocker.patch.object(inmemory_store_mocked, "conn").cursor.return_value = mock_cursor

    # Call create_metadata_table
    inmemory_store_mocked.create_metadata_table()

    # Assert the expected query was executed on the mock cursor
    mock_cursor.execute.assert_called_once_with(
        f"""
                CREATE TABLE IF NOT EXISTS {POSTGRESQL_TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    execution_block VARCHAR(255) DEFAULT NULL,
                    json_hash CHAR(64) UNIQUE
                );
            """
    )


def test_delete_postgres_table(mocker):
    """Test for delete_postgres_table"""
    # Mock cursor execution to avoid actual database interaction
    inmemory_store_mocked = PostgresConnector()

    mock_cursor = mocker.Mock()
    mocker.patch.object(inmemory_store_mocked, "conn").cursor.return_value = mock_cursor

    # Call delete_postgres_table
    inmemory_store_mocked.delete_postgres_table()

    # Assert the expected query was executed on the mock cursor
    mock_cursor.execute.assert_called_once_with(f"DROP TABLE IF EXISTS {POSTGRESQL_TABLE_NAME}")
