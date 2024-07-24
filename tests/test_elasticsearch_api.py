"""Module to test insertMetadata.py"""

import json
from datetime import datetime  # Assuming connection_established_at uses datetime

from ska_sdp_dataproduct_api.components.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.configuration.settings import ELASTICSEARCH_METADATA_SCHEMA_FILE
from ska_sdp_dataproduct_api.utilities.helperfunctions import DPDAPIStatus
from tests.mock_elasticsearch_api import MockElasticsearch

DPD_API_Status = DPDAPIStatus()


def test_create_schema():
    """Method to test creation of schema."""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()

    with open(
        ELASTICSEARCH_METADATA_SCHEMA_FILE,
        "r",
        encoding="UTF-8",
    ) as schema_file:
        schema = json.loads(schema_file.read())
    metadata_store.create_schema_if_not_existing(
        index="localhost-sdp-dataproduct-dashboard-dev-v1"
    )
    response = metadata_store.es_client.indices.get(
        index="localhost-sdp-dataproduct-dashboard-dev-v1"
    )
    assert response == schema


def test_insert_metadata():
    """Method to test insertion of metadata."""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()

    with open(
        "tests/test_files/example_files/example_metadata.json",
        "r",
        encoding="UTF-8",
    ) as document_file:
        document = document_file.read()

    metadata_store.insert_metadata_in_search_store(document)
    response = metadata_store.es_client.get(
        index="localhost-sdp-dataproduct-dashboard-dev-v1", id=1
    )

    assert response == json.loads(document)


def test_update_dataproduct_list():
    """Method to test insertion of metadata."""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()

    with open(
        "tests/test_files/example_files/example_metadata.json",
        "r",
        encoding="UTF-8",
    ) as document_file:
        metadata_file = json.loads(document_file.read())

    metadata_store.add_dataproduct(
        metadata_file=metadata_file,
    )

    expected_value = [
        {
            "id": 1,
            "execution_block": "eb-m001-20191031-12345",
        }
    ]

    assert metadata_store.metadata_list == expected_value


def test_search_metadata():
    """Method to test search of metadata"""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()
    metadata_store.es_client.ping = lambda: True
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
    metadata_list = metadata_store.filter_data(mui_data_grid_filter_model, search_panel_options)

    expected_value = [
        {
            "execution_block": "eb-m001-20191031-12345",
            "date_created": "2019-10-31",
            "dataproduct_file": "product",
            "metadata_file": "product",
            "interface": "http://schema.skao.int",
            "id": 1,
        }
    ]

    assert metadata_list == expected_value


def test_search_metadata_default_value():
    """Method to test search of metadata if metadata_key_value_pair is None"""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()
    metadata_store.es_client.ping = lambda: True
    mui_data_grid_filter_model = {}
    search_panel_options = {
        "items": [
            {"field": "date_created", "operator": "greaterThan", "value": ""},
            {"field": "date_created", "operator": "lessThan", "value": ""},
            {"field": "formFields", "keyPairs": [{"keyPair": "", "valuePair": ""}]},
        ],
        "logicOperator": "and",
    }
    metadata_list = metadata_store.filter_data(mui_data_grid_filter_model, search_panel_options)

    expected_value = [
        {
            "execution_block": "eb-m001-20191031-12345",
            "date_created": "2019-10-31",
            "dataproduct_file": "product",
            "metadata_file": "product",
            "interface": "http://schema.skao.int",
            "id": 1,
        }
    ]

    assert metadata_list == expected_value


def test_status(mocker):
    """Tests the status method with different scenarios."""

    mocked_self = ElasticsearchMetadataStore()

    # Mock attributes
    host = "localhost"
    port = 9200
    user = "elastic"
    running = True
    connection_established_at = datetime.now()
    cluster_info = {"name": "my_cluster"}

    # Mock attributes
    mocker.patch.object(mocked_self, "host", host)
    mocker.patch.object(mocked_self, "port", port)
    mocker.patch.object(mocked_self, "user", user)
    mocker.patch.object(mocked_self, "elasticsearch_running", running)
    mocker.patch.object(mocked_self, "connection_established_at", connection_established_at)
    mocker.patch.object(mocked_self, "cluster_info", cluster_info)

    # Call the method
    response = mocked_self.status()

    # Assert expected response
    assert response == {
        "metadata_store_in_use": "ElasticsearchMetadataStore",
        "url": "https://localhost:9200",
        "user": user,
        "running": running,
        "connection_established_at": mocked_self.connection_established_at,
        "number_of_dataproducts": 0,
        "indices": "localhost-sdp-dataproduct-dashboard-dev-v1",
        "cluster_info": cluster_info,
    }
