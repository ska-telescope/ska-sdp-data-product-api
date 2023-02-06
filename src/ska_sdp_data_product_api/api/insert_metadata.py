"""Module to insert data into Elasticsearch instance."""
import json

from elasticsearch import Elasticsearch


class InsertMetadata:
    """Class to insert data into Elasticsearch instance."""

    es_client = None

    def create_elasticsearch_connection(
        self, host: str, cert_location: str, username: str, password: str
    ):
        """Method to connect to Elasticsearch instance."""
        self.es_client = Elasticsearch(
            hosts=host, ca_certs=cert_location, basic_auth=(username, password)
        )

    def create_index_from_schema(self, index_name: str, schema: dict):
        """Method to create Schema from Schema Dict."""
        self.es_client.indices.create(  # pylint: disable=E1123
            index=index_name, ignore=400, body=schema
        )

    def insert_metadata(self, index_name: str, document: dict):
        """Method to insert data into Elasticsearch given index.
        It will also create schema if no schema is given."""
        self.es_client.index(index=index_name, document=json.dumps(document))

    def retrieve_metadata(self, index_name: str, i_d: int):
        """Basic get data method given index and id."""
        return self.es_client.get(index=index_name, id=i_d)
