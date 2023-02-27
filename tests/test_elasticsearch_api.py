"""Module to test insertMetadata.py"""

import json

from ska_sdp_data_product_api.core.settings import METADATA_ES_SCHEMA_FILE
from ska_sdp_data_product_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from tests.mock_elasticsearch_api import MockElasticsearch


def test_create_schema():
    """Method to test creation of schema."""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()

    with open(
        METADATA_ES_SCHEMA_FILE,
        "r",
        encoding="UTF-8",
    ) as schema_file:
        schema = json.loads(schema_file.read())
    metadata_store.create_schema_if_not_existing(index="sdp_meta_data")
    response = metadata_store.es_client.indices.get(index="sdp_meta_data")
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

    metadata_store.insert_metadata(document)
    response = metadata_store.es_client.get(index="sdp_meta_data", id=1)

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

    metadata_store.update_dataproduct_list(
        metadata_file=metadata_file,
    )

    expected_value = [
        {
            "id": 1,
            "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
            "execution_block": "eb-m001-20191031-12345",
        }
    ]

    assert metadata_store.metadata_list == expected_value


def test_search_metadata():
    """Method to test search of metadata"""
    metadata_store = ElasticsearchMetadataStore()
    metadata_store.es_client = MockElasticsearch()

    metadata_list = metadata_store.search_metadata(
        start_date="2020-01-01",
        end_date="2100-01-01",
        metadata_key="*",
        metadata_value="*",
    )

    expected_value = [
        {
            "id": 1,
            "interface": "http://schema.skao.int",
            "execution_block": "eb-m001-20191031-12345",
            "date_created": "2019-10-31",
            "dataproduct_file": "product",
            "metadata_file": "product",
        }
    ]

    assert json.loads(metadata_list) == expected_value