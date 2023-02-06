"""Module to test insertMetadata.py"""

import json

from ska_sdp_data_product_api.api.insert_metadata import InsertMetadata


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
        return self.values[index]["schema"]


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
    insert_metadata = InsertMetadata()
    insert_metadata.es_client = MockElasticsearch()

    with open(
        "tests/test_files/example_files/example_schema.json",
        "r",
        encoding="UTF-8",
    ) as schema_file:
        schema = schema_file.read()

    insert_metadata.create_index_from_schema("example_index", schema)

    response = insert_metadata.es_client.indices.get(index="example_index")

    assert response == schema


def test_insert_metadata():
    """Method to test insertion of metadata."""
    insert_metadata = InsertMetadata()
    insert_metadata.es_client = MockElasticsearch()

    with open(
        "tests/test_files/example_files/example_metadata.json",
        "r",
        encoding="UTF-8",
    ) as document_file:
        document = document_file.read()

    insert_metadata.insert_metadata("example_index", document)

    response = insert_metadata.retrieve_metadata("example_index", 1)

    assert response == document
