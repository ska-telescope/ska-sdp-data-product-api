"""Module to test insertMetadata.py"""

import json

import elasticsearch


class MockIndices:
    """Mocked Indices."""

    def __init__(self, values):
        """Init the fake Indices."""
        self.values = values

    def create(self, index, ignore, body):  # pylint: disable=W0613
        """Create Index."""
        self.values[index] = {"schema": body}

    def get(self, index):
        """Retrieve Index."""
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

    def search(self, index, body):  # pylint: disable=W0613
        """Mock search results."""
        mock_results = {
            "took": 14,
            "timed_out": False,
            "_shards": {
                "total": 1,
                "successful": 1,
                "skipped": 0,
                "failed": 0,
            },
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "max_score": 1.7509373,
                "hits": [
                    {
                        "_index": "localhost-sdp-dataproduct-dashboard-dev-v1",
                        "_id": "wRJENYYBOwlRnNXHy2_p",
                        "_score": 1.7509373,
                        "_source": {
                            "interface": "http://schema.skao.int",
                            "execution_block": "eb-m001-20191031-12345",
                            "date_created": "2019-10-31",
                            "dataproduct_file": "product",
                            "metadata_file": "product",
                        },
                    },
                ],
            },
        }
        return mock_results
