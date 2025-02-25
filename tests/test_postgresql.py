"""Module to test PostgresConnector"""
import logging
import pathlib
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ska_dataproduct_api.components.annotations.annotation import DataProductAnnotation
from ska_dataproduct_api.components.metadata.metadata import DataProductMetadata
from ska_dataproduct_api.components.pv_interface.pv_interface import PVInterface
from ska_dataproduct_api.components.store.persistent.postgresql import (
    PGMetadataStore,
    PostgresConnector,
)
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


def mock_get_data_by_uid(uid):
    """Retrieves mock data based on the provided UUID.

    Args:
        uid (str): The UUID to query.

    Returns:
        dict: A dictionary containing the mock data, or None if the UUID is invalid.

    This function is designed to simulate the retrieval of data from an external source.
    It returns a dictionary containing a 'dataproduct_file' key with a file path for the
    valid 'valid_uid'. For any other UUID, it returns None.
    """
    if uid == "valid_uid":
        return {"dataproduct_file": "path/to/another_file.txt"}
    return None


def mock_get_annotations_by_uid(uid):
    """Retrieves mock annotation based on the provided uid.

    Args:
        uid (str): The data product uid.

    Returns:
        A list of objects with type DataProductAnnotation if uid is valid
        or an empty list if the id is invalid.

    This function is designed to simulate the retrieval of data from an external source.
    It returns a list of DataProductAnnotations. For any other uid, it returns an empty list.
    """
    if uid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c":
        return mock_db.retrieve_annotations_by_uid(uid)
    return []


# Test cases
def test_status(mocked_postgres_connector):
    """Mock connection logic for setup and teardown"""
    status = mocked_postgres_connector["connector"].status()
    expected_status = {
        "configured": True,
        "dbname": "test_db",
        "host": "localhost",
        "port": 5432,
        "running": True,
        "schema": "public",
        "user": "test_user",
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


# get_data_product_file_paths tests
def test_valid_execution_block(mocked_postgres_connector):
    """Tests successful retrieval of data product file paths for a valid execution block."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    identifier = DataProductIdentifier(execution_block="valid_execution_block")
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
get_data_by_execution_block",
        side_effect=mock_get_data_by_execution_block,
    ):
        result = metadata_store.get_data_product_file_paths(identifier)
        assert result == [pathlib.Path("path/to/file.txt")]


def test_valid_uid(mocked_postgres_connector):
    """Tests successful retrieval of data product file paths for a valid UUID."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    identifier = DataProductIdentifier(uid="valid_uid")
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
get_data_by_uid",
        side_effect=mock_get_data_by_uid,
    ):
        result = metadata_store.get_data_product_file_paths(identifier)
        assert result == [pathlib.Path("path/to/another_file.txt")]


def test_invalid_identifier(mocked_postgres_connector):
    """Tests that an empty list is returned for an invalid DataProductIdentifier."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    identifier = DataProductIdentifier(
        execution_block="invalid_execution_block", uid="invalid_uid"
    )
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
get_data_by_execution_block",
        side_effect=mock_get_data_by_execution_block,
    ), patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
get_data_by_uid",
        side_effect=mock_get_data_by_uid,
    ):
        result = metadata_store.get_data_product_file_paths(identifier)
        assert not result


def test_reindex_persistent_volume(mocked_postgres_connector):
    """Tests if the reload_all_data_products_in_index can be executed, the call to the PosgreSQL
    cursor is mocked, so the expected return of the number if items in the db is only 1"""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    pv_interface = PVInterface()
    metadata_store.reload_all_data_products_in_index(pv_index=pv_interface.pv_index)

    assert metadata_store.number_of_date_products_in_table == 1


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
    data_product_metadata_instance: DataProductMetadata = DataProductMetadata(data_store="dpd")
    data_product_metadata_instance.load_metadata_from_class(test_metadata)

    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )

    with patch.object(metadata_store, "check_metadata_exists_by_hash", return_value=False):
        with patch.object(
            metadata_store,
            "get_metadata_id_by_uid",
            return_value=None,
        ):
            metadata_store.save_metadata_to_postgresql(data_product_metadata_instance)
            assert metadata_store.number_of_date_products_in_table == 1

    with patch.object(metadata_store, "check_metadata_exists_by_hash", return_value=False):
        with patch.object(
            metadata_store,
            "get_metadata_id_by_uid",
            return_value=1,
        ):
            metadata_store.save_metadata_to_postgresql(data_product_metadata_instance)
            assert metadata_store.number_of_date_products_in_table == 1


def test_get_metadata(mocked_postgres_connector):
    """Tests if the reload_all_data_products_in_index can be executed, the call to the PosgreSQL
    cursor is mocked, so the expected return of the number if items in the db is only 1"""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
get_data_by_uid",
        side_effect=mock_get_data_by_uid,
    ):
        result = metadata_store.get_metadata("valid_uid")
        assert result == {"dataproduct_file": "path/to/another_file.txt"}

        result = metadata_store.get_metadata("invalid_uid")
        assert result == {}


def test_save_annotation_create(mocked_postgres_connector):
    """Tests if annotation is successfully saved in database."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    data_product_annotation = {
        "data_product_uid": "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c",
        "annotation_text": "test annotation testjkuhkuhkjhk",
        "user_principal_name": "test.user@skao.int",
        "timestamp_created": "2024-11-13T14:35:00",
        "timestamp_modified": "2024-11-13T14:35:00",
    }
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
save_annotation",
        return_value=None,
    ):
        assert metadata_store.save_annotation(data_product_annotation) is None


def test_save_annotation_update(mocked_postgres_connector):
    """Tests if annotation is successfully saved in database."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    data_product_annotation = {
        "annotation_text": "Updated text test annotation testjkuhkuhkjhk",
        "user_principal_name": "test.user@skao.int",
        "timestamp_modified": "2024-11-25T14:35:00",
        "annotation_id": 1,
    }
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
save_annotation",
        return_value=None,
    ):
        assert metadata_store.save_annotation(data_product_annotation) is None


def test_retrieve_annotations_by_uid_valid_uid(mocked_postgres_connector):
    """Tests if annotation is successfully retrieved given a valid id."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
retrieve_annotations_by_uid",
        side_effect=mock_get_annotations_by_uid,
    ):
        result = metadata_store.retrieve_annotations_by_uid("1f8250d0-0e2f-2269-1d9a-ad465ae15d5c")
        assert len(result) > 0
        assert isinstance(result[0], DataProductAnnotation)
        assert result[0].data_product_uid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"
        assert result[1].data_product_uid == "1f8250d0-0e2f-2269-1d9a-ad465ae15d5c"


def test_retrieve_annotations_by_uid_invalid_uid(mocked_postgres_connector):
    """Tests if annotation is successfully retrieved given a valid id."""
    metadata_store = PGMetadataStore(
        db=mocked_postgres_connector["connector"],
        science_metadata_table_name="my_table",
        annotations_table_name="annotations_table",
        dlm_schema="dlm",
        dlm_data_item_table_name="data_item",
    )
    with patch(
        "ska_dataproduct_api.components.store.persistent.postgresql.PGMetadataStore.\
retrieve_annotations_by_uid",
        side_effect=mock_get_annotations_by_uid,
    ):
        result = metadata_store.retrieve_annotations_by_uid("hello")
        assert len(result) == 0
