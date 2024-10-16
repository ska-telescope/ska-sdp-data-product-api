"""Module to test ElasticsearchMetadataStore"""

import pathlib
from datetime import datetime

from ska_dataproduct_api.components.search.elasticsearch.elasticsearch import (
    ElasticsearchMetadataStore,
)
from ska_dataproduct_api.components.store.store_factory import select_metadata_store_class
from ska_dataproduct_api.configuration.settings import (
    CONFIGURATION_FILES_PATH,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_INDICES,
    ELASTICSEARCH_METADATA_SCHEMA_FILE,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_USER,
)
from ska_dataproduct_api.utilities.helperfunctions import DPDAPIStatus
from tests.mock_elasticsearch_api import MockElasticsearch

DPD_API_Status = DPDAPIStatus()
metadata_store = select_metadata_store_class()

# pylint: disable=duplicate-code


def test_status(mocker):
    """Tests the status method with different scenarios."""

    mocked_metadata_store = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )

    # Mock attributes
    host = "localhost"
    port = 9200
    user = "elastic"
    running = True
    connection_established_at = datetime.now()
    cluster_info = {"name": "my_cluster"}

    # Mock attributes
    mocker.patch.object(mocked_metadata_store, "host", host)
    mocker.patch.object(mocked_metadata_store, "port", port)
    mocker.patch.object(mocked_metadata_store, "user", user)
    mocker.patch.object(mocked_metadata_store, "elasticsearch_running", running)
    mocker.patch.object(
        mocked_metadata_store, "connection_established_at", connection_established_at
    )
    mocker.patch.object(mocked_metadata_store, "cluster_info", cluster_info)

    # Call the method
    response = mocked_metadata_store.status()

    # Assert expected response
    assert "metadata_store_in_use" in response
    assert "url" in response
    assert "user" in response
    assert "running" in response
    assert "connection_established_at" in response
    assert "number_of_dataproducts" in response
    assert "indices" in response
    assert "cluster_info" in response

    # Check specific values
    assert response["metadata_store_in_use"] == "ElasticsearchMetadataStore"
    assert response["user"] == user
    assert response["running"] == running


def test_no_ca_cert_configured():
    """Test for when no ca cert is available"""
    es_store = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )
    es_store.load_ca_cert(config_file_path=CONFIGURATION_FILES_PATH, ca_cert="")
    assert es_store.ca_cert is None


def test_search_metadata():
    """Method to test search of metadata"""
    search_store = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )
    search_store.es_client = MockElasticsearch()
    search_store.es_client.ping = lambda: True
    mui_data_grid_filter_model = {
        "items": [
            {
                "field": "execution_block",
                "operator": "contains",
                "id": 51411,
                "value": "m001",
                "fromInput": ":r4l:",
            }
        ],
        "logicOperator": "and",
        "quickFilterValues": [],
        "quickFilterLogicOperator": "and",
    }
    search_panel_options = {
        "items": [
            {"field": "date_created", "operator": "greaterThan", "value": ""},
            {"field": "date_created", "operator": "lessThan", "value": ""},
            {"field": "formFields", "keyPairs": [{"keyPair": "", "valuePair": ""}]},
        ],
        "logicOperator": "and",
    }
    metadata_list = search_store.filter_data(
        mui_data_grid_filter_model=mui_data_grid_filter_model,
        search_panel_options=search_panel_options,
        users_user_group_list=[],
    )

    expected_value = [
        {
            "execution_block": "eb-m001-20191031-12345",
            "date_created": "2019-10-31",
            "dataproduct_file": "product",
            "metadata_file": "product",
            "uuid": "f0b91aa5-d54b-e11a-410e-3e4edca5346f",
            "interface": "http://schema.skao.int",
            "id": 1,
        }
    ]

    assert metadata_list == expected_value


def test_search_metadata_default_value():
    """Method to test search of metadata if metadata_key_value_pair is None"""
    search_store = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )
    search_store.es_client = MockElasticsearch()
    search_store.es_client.ping = lambda: True
    mui_data_grid_filter_model = {}
    search_panel_options = {
        "items": [
            {"field": "date_created", "operator": "greaterThan", "value": ""},
            {"field": "date_created", "operator": "lessThan", "value": ""},
            {"field": "formFields", "keyPairs": [{"keyPair": "", "valuePair": ""}]},
        ],
        "logicOperator": "and",
    }
    metadata_list = search_store.filter_data(
        mui_data_grid_filter_model=mui_data_grid_filter_model,
        search_panel_options=search_panel_options,
        users_user_group_list=[],
    )

    expected_value = [
        {
            "execution_block": "eb-m001-20191031-12345",
            "date_created": "2019-10-31",
            "dataproduct_file": "product",
            "metadata_file": "product",
            "uuid": "f0b91aa5-d54b-e11a-410e-3e4edca5346f",
            "interface": "http://schema.skao.int",
            "id": 1,
        }
    ]

    assert metadata_list == expected_value


def test_save_ca_cert_to_file_success():
    """Tests save_ca_cert_to_file."""

    search_store = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )
    search_store.es_client = MockElasticsearch()
    search_store.es_client.ping = lambda: True

    ca_cert_content = b"certificate_content"
    config_file_path = pathlib.Path("/tmp")
    ca_cert = pathlib.Path("ca.crt")

    result = search_store.save_ca_cert_to_file(ca_cert_content, config_file_path, ca_cert)
    test_file_path = pathlib.Path(config_file_path / ca_cert)
    test_file_path.unlink()
    assert result is True

    ca_cert_content = b"certificate_content"
    config_file_path = pathlib.Path("/non_existing_path")
    ca_cert = pathlib.Path("ca.crt")

    result = search_store.save_ca_cert_to_file(ca_cert_content, config_file_path, ca_cert)
    assert result is False
