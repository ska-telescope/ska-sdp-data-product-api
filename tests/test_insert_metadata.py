import json

from ska_sdp_data_product_api.api.insert_metadata import InsertMetadata

class Indices:
    """Mocked Indices."""
    
    def __init__(self, values):
        self.values = values

    def create(self, index, ignore, body):
        self.values[index] = {'schema': body}
    
    def get(self, index):
        return self.values[index]['schema']


class MockElasticsearch:
    """Mocked Elasticsearch."""

    def __init__(self):
        """Init the fake Elasticsearch."""
        self.values = {}
        self.count = 1
        self.indices = Indices(self.values)


    def index(self, index, document):
        """Set a value."""
        if index not in self.values.keys():
            self.values[index] = {'schema': json.dumps(document)}
        self.values[index][self.count] = json.loads(document)
        self.count+=1
        

    def get(self, index, id):
        """Get a value or None."""
        return self.values[index][id]


############################################################################################################

def test_create_schema():
    insertMetadata = InsertMetadata()
    insertMetadata.es_client = MockElasticsearch()

    with open('tests/test_files/example_files/example_schema.json', 'r') as schema_file:
        schema = schema_file.read()
    
    insertMetadata.create_index_from_schema("example_index", schema)

    response = insertMetadata.es_client.indices.get("example_index")

    assert response == schema


def test_insert_metadata():
    insertMetadata = InsertMetadata()
    insertMetadata.es_client = MockElasticsearch()

    with open('tests/test_files/example_files/example_metadata.json', 'r') as document_file:
        document = document_file.read()

    insertMetadata.insert_metadata("example_index", document)

    response = insertMetadata.retrieve_metadata("example_index", 1)

    assert response == document