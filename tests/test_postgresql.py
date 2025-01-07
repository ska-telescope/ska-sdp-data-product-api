"""Module to test PostgresConnector"""
import logging
import pathlib
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation
from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_dataproduct_api.utilities.helperfunctions import DataProductIdentifier
from tests.mock_postgressql import MockPostgresSQL

# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

logger = logging.getLogger(__name__)

mock_db = MockPostgresSQL()
mock_db.initialize_database()


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
            annotations_table_name="annotations_table",
        )

        # Set any additional properties you want to control
        connector.postgresql_running = True
        connector.postgresql_version = "mocked"
        connector.date_modified = datetime.now()
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        yield {"connector": connector, "cursor": mock_cursor}


# Mock functions
def mock_get_data_by_execution_block(execution_block):
    """Retrieves mock data based on the provided execution block.

    Args:
        execution_block (str): The execution block to query.

    Returns:
        dict: A dictionary containing the mock data, or None if the execution block is invalid.

    This function is designed to simulate the retrieval of data from an external source.
    It returns a dictionary containing a 'dataproduct_file' key with a file path for the
    valid 'valid_execution_block'. For any other execution block, it returns None.
    """
    if execution_block == "valid_execution_block":
        return {"dataproduct_file": "path/to/file.txt"}
    return None


def mock_get_data_by_uuid(uuid):
    """Retrieves mock data based on the provided UUID.

    Args:
        uuid (str): The UUID to query.

    Returns:
        dict: A dictionary containing the mock data, or None if the UUID is invalid.

    This function is designed to simulate the retrieval of data from an external source.
    It returns a dictionary containing a 'dataproduct_file' key with a file path for the
    valid 'valid_uuid'. For any other UUID, it returns None.
    """
    if uuid == "valid_uuid":
        return {"dataproduct_file": "path/to/another_file.txt"}
    return None


def mock_get_annotations_by_uuid(uuid):
    """Retrieves mock annotation based on the provided uuid.

    Args:
        uuid (str): The data product uuid.

    Returns:
        A list of objects with type DataProductAnnotation if uuid is valid
        or an empty list if the id is invalid.

    This function is designed to simulate the retrieval of data from an external source.
    It returns a list of DataProductAnnotations. For any other uuid, it returns an empty list.
    """
    if uuid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c":
        return mock_db.retrieve_annotations_by_uuid(uuid)
    return []


# Test cases
def test_status(mocked_postgres_connector):
    """Mock connection logic for setup and teardown"""
    status = mocked_postgres_connector["connector"].status()
    expected_status = {
        "store_type": "Persistent PosgreSQL metadata store",
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "configured": True,
        "running": True,
        "indexing": False,
        "dbname": "test_db",
        "schema": "public",
        "table_name": "my_table",
        "annotations_table_name": "annotations_table",
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

    mocked_postgres_connector["connector"].host = ""
    with pytest.raises(ConnectionError):
        connection_string = mocked_postgres_connector["connector"].build_connection_string()


# calculate_metadata_hash tests
def test_calculate_metadata_hash(mocked_postgres_connector):
    """Tests the calculation of a metadata hash."""
    # Test with a simple JSON object
    metadata_json = {
        "key1": "value1",
        "key2": [1, 2, 3],
        "key3": {"nested_key": "nested_value"},
    }
    expected_hash = "3f8ac9cedca91c2556da09a4448bf629fd3b3049e07c4d7220e76b8e12542d33"

    actual_hash = mocked_postgres_connector["connector"].calculate_metadata_hash(metadata_json)
    assert actual_hash == expected_hash

    # Test with an empty JSON object
    metadata_json = {}
    expected_hash = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"

    actual_hash = mocked_postgres_connector["connector"].calculate_metadata_hash(metadata_json)
    assert actual_hash == expected_hash


# get_data_product_file_paths tests
def test_valid_execution_block(mocked_postgres_connector):
    """Tests successful retrieval of data product file paths for a valid execution block."""
    identifier = DataProductIdentifier(execution_block="valid_execution_block")
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
get_data_by_execution_block",
        side_effect=mock_get_data_by_execution_block,
    ):
        result = mocked_postgres_connector["connector"].get_data_product_file_paths(identifier)
        assert result == [pathlib.Path("path/to/file.txt")]


def test_valid_uuid(mocked_postgres_connector):
    """Tests successful retrieval of data product file paths for a valid UUID."""
    identifier = DataProductIdentifier(uuid="valid_uuid")
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
get_data_by_uuid",
        side_effect=mock_get_data_by_uuid,
    ):
        result = mocked_postgres_connector["connector"].get_data_product_file_paths(identifier)
        assert result == [pathlib.Path("path/to/another_file.txt")]


def test_invalid_identifier(mocked_postgres_connector):
    """Tests that an empty list is returned for an invalid DataProductIdentifier."""
    identifier = DataProductIdentifier(
        execution_block="invalid_execution_block", uuid="invalid_uuid"
    )
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
get_data_by_execution_block",
        side_effect=mock_get_data_by_execution_block,
    ), patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
get_data_by_uuid",
        side_effect=mock_get_data_by_uuid,
    ):
        result = mocked_postgres_connector["connector"].get_data_product_file_paths(identifier)
        assert result == []


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
    data_product_metadata_instance: DataProductMetadata = DataProductMetadata()
    data_product_metadata_instance.load_metadata_from_class(test_metadata)

    with patch.object(
        mocked_postgres_connector["connector"], "check_metadata_exists_by_hash", return_value=False
    ):
        with patch.object(
            mocked_postgres_connector["connector"],
            "check_metadata_exists_by_uuid",
            return_value=None,
        ):
            mocked_postgres_connector["connector"].save_metadata_to_postgresql(
                data_product_metadata_instance
            )
            assert mocked_postgres_connector["connector"].number_of_dataproducts == 1

    with patch.object(
        mocked_postgres_connector["connector"], "check_metadata_exists_by_hash", return_value=False
    ):
        with patch.object(
            mocked_postgres_connector["connector"],
            "check_metadata_exists_by_uuid",
            return_value=1,
        ):
            mocked_postgres_connector["connector"].save_metadata_to_postgresql(
                data_product_metadata_instance
            )
            assert mocked_postgres_connector["connector"].number_of_dataproducts == 1


def test_get_metadata(mocked_postgres_connector):
    """Tests if the reindex_persistent_volume can be executed, the call to the PosgreSQL cursor
    is mocked, so the expected return of the number if items in the db is only 1"""
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
get_data_by_uuid",
        side_effect=mock_get_data_by_uuid,
    ):
        result = mocked_postgres_connector["connector"].get_metadata("valid_uuid")
        assert result == {"dataproduct_file": "path/to/another_file.txt"}

        result = mocked_postgres_connector["connector"].get_metadata("invalid_uuid")
        assert result == {}


def test_save_annotation_create(mocked_postgres_connector):
    """Tests if annotation is successfully saved in database."""
    data_product_annotation = {
        "data_product_uuid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
        "annotation_text": "test annotation testjkuhkuhkjhk",
        "user_principal_name": "test.user@skao.int",
        "timestamp_created": "2024-11-13T14:35:00",
        "timestamp_modified": "2024-11-13T14:35:00",
    }
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
save_annotation",
        return_value=None,
    ):
        assert (
            mocked_postgres_connector["connector"].save_annotation(data_product_annotation) is None
        )


def test_save_annotation_update(mocked_postgres_connector):
    """Tests if annotation is successfully saved in database."""
    data_product_annotation = {
        "annotation_text": "Updated text test annotation testjkuhkuhkjhk",
        "user_principal_name": "test.user@skao.int",
        "timestamp_modified": "2024-11-25T14:35:00",
        "annotation_id": 1,
    }
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
save_annotation",
        return_value=None,
    ):
        assert (
            mocked_postgres_connector["connector"].save_annotation(data_product_annotation) is None
        )


def test_retrieve_annotations_by_uuid_valid_uuid(mocked_postgres_connector):
    """Tests if annotation is successfully retrieved given a valid id."""
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
retrieve_annotations_by_uuid",
        side_effect=mock_get_annotations_by_uuid,
    ):
        result = mocked_postgres_connector["connector"].retrieve_annotations_by_uuid(
            "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
        )
        assert len(result) > 0
        assert isinstance(result[0], DataProductAnnotation)
        assert result[0].data_product_uuid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
        assert result[1].data_product_uuid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"


def test_retrieve_annotations_by_uuid_invalid_uuid(mocked_postgres_connector):
    """Tests if annotation is successfully retrieved given a valid id."""
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PostgresConnector.\
retrieve_annotations_by_uuid",
        side_effect=mock_get_annotations_by_uuid,
    ):
        result = mocked_postgres_connector["connector"].retrieve_annotations_by_uuid("hello")
        assert len(result) == 0
