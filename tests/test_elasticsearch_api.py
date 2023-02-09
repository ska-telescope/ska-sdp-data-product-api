"""Module to test insertMetadata.py"""

import json

import elasticsearch

from ska_sdp_data_product_api.core.settings import METADATA_ES_SCHEMA_FILE
from ska_sdp_data_product_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
    update_dataproduct_list,
)


class MockIndices:
    """Mocked Indices."""

    def __init__(self, values):
        """Init the fake Indices."""
        self.values = values

    def create(self, index, ignore, body):  # pylint: disable=W0613
        """Create Index."""
        self.values[index] = {"schema": body}

    def get(self, index):
        """Retrive Index."""
        if index in self.values:
            return self.values[index]["schema"]
        raise elasticsearch.NotFoundError("message", "meta", "body")


class MockElasticsearch:
    """Mocked Elasticsearch."""

    def __init__(self):
        """Init the fake Elasticsearch."""
        self.values = {}
        self.count = 1
        self.indices = MockIndices(self.values)

    def index(self, index, document):
        """Set a value."""
        if index not in self.values:
            self.values[index] = {"schema": json.dumps(document)}
        self.values[index][self.count] = json.loads(document)
        self.count += 1

    def get(self, index, id):  # pylint: disable=W0622,C0103
        """Get a value or None."""
        return self.values[index][id]


#############################################################################


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
    metadata_list = []

    with open(
        "tests/test_files/example_files/example_metadata.json",
        "r",
        encoding="UTF-8",
    ) as document_file:
        metadata_file = json.loads(document_file.read())

    list_id = 1

    update_dataproduct_list(
        metadata_list=metadata_list,
        metadata_file=metadata_file,
        list_id=list_id,
    )

    expected_value = [
        {
            "id": 1,
            "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
            "execution_block": "eb-m001-20191031-12345",
        }
    ]

    assert metadata_list == expected_value
