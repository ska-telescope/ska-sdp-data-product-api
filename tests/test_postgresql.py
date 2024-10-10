"""Module to test PostgresConnector"""
import datetime
import pathlib
from unittest.mock import MagicMock, patch

import pytest
from psycopg import OperationalError

from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector

# pylint: disable=redefined-outer-name
# pylint: disable=protected-access


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
        connector.date_modified = datetime.datetime.now()
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
        "last_metadata_update_time": mocked_postgres_connector["connector"].date_modified,
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

    result = mocked_postgres_connector["connector"].get_data_product_file_paths(execution_block)

    assert result == expected_file_path


def test_get_data_product_file_path_not_found(mocked_postgres_connector):
    """Tests when file path is not found."""
    execution_block = "test_block"

    mocked_postgres_connector["cursor"].fetchone.return_value = ({},)

    result = mocked_postgres_connector["connector"].get_data_product_file_paths(execution_block)

    assert result == {}


def test_reindex_persistent_volume(mocked_postgres_connector):
    """Tests if the reindex_persistent_volume can be executed, the call to the PosgreSQL cursor
    is mocked, so the expected return of the number if items in the db is only 1"""

    mocked_postgres_connector["connector"].reindex_persistent_volume()

    assert mocked_postgres_connector["connector"].number_of_dataproducts == 1
    assert len(mocked_postgres_connector["connector"].list_of_data_product_paths) == 18
    assert mocked_postgres_connector["connector"].indexing is False


def test_save_metadata_to_postgresql(mocked_postgres_connector):
    """Tests if"""

    test_metadata = {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "execution_block": "eb-test-20240824-123321",
        "context": {"observer": "Andre", "intent": "Postman Test", "notes": "Test note"},
        "config": {
            "processing_block": "",
            "processing_script": "",
            "image": "",
            "version": "ssss",
            "commit": "",
            "cmdline": "",
        },
        "files": [],
        "obscore": {
            "access_estsize": 0,
            "access_format": "application/unknown",
            "access_url": "0",
            "calib_level": 0,
            "dataproduct_type": "MS",
            "facility_name": "SKA",
            "instrument_name": "SKA-LOW",
            "o_ucd": "stat.fourier",
            "obs_collection": "Unknown",
            "obs_id": "",
            "obs_publisher_did": "",
            "pol_states": "XX/XY/YX/YY",
            "pol_xel": 0,
            "s_dec": 0,
            "s_ra": 0.0,
            "t_exptime": 5.0,
            "t_max": 57196.962848574476,
            "t_min": 57196.96279070411,
            "t_resolution": 0.9,
            "target_name": "",
        },
    }

    with patch.object(
        mocked_postgres_connector["connector"], "check_metadata_exists_by_hash", return_value=False
    ):
        with patch.object(
            mocked_postgres_connector["connector"],
            "check_metadata_exists_by_execution_block",
            return_value=None,
        ):
            mocked_postgres_connector["connector"].save_metadata_to_postgresql(test_metadata)
            assert mocked_postgres_connector["connector"].number_of_dataproducts == 1
            assert mocked_postgres_connector["connector"].conn.commit.call_count == 2

    with patch.object(
        mocked_postgres_connector["connector"], "check_metadata_exists_by_hash", return_value=False
    ):
        with patch.object(
            mocked_postgres_connector["connector"],
            "check_metadata_exists_by_execution_block",
            return_value=1,
        ):
            mocked_postgres_connector["connector"].save_metadata_to_postgresql(test_metadata)
            assert mocked_postgres_connector["connector"].number_of_dataproducts == 1
            assert mocked_postgres_connector["connector"].conn.commit.call_count == 3


def test_get_metadata(mocked_postgres_connector):
    """Tests if the reindex_persistent_volume can be executed, the call to the PosgreSQL cursor
    is mocked, so the expected return of the number if items in the db is only 1"""

    mocked_postgres_connector["cursor"].fetchone.return_value = ({"mockData"},)

    execution_block = "eb-test-20240824-123321"
    result = mocked_postgres_connector["connector"].get_metadata(execution_block)

    assert result == {"mockData"}


def test_postgresql_query_operational_error(mocked_postgres_connector):
    """Simulate a connection error that occurs multiple times"""

    def raise_operational_error():
        raise OperationalError("Connection error")

    mocked_postgres_connector["connector"].conn.cursor = raise_operational_error
    mocked_postgres_connector["connector"].retry_delay = 1

    with pytest.raises(OperationalError):
        mocked_postgres_connector["connector"]._postgresql_query("SELECT 1;", ())

    assert mocked_postgres_connector["connector"].postgresql_running is False
