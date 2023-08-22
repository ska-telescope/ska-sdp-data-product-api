"""Module to insert data into Elasticsearch instance."""
import json
import logging

import elasticsearch
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.core.settings import METADATA_ES_SCHEMA_FILE
from ska_sdp_dataproduct_api.metadatastore.datastore import Store

logger = logging.getLogger(__name__)


class ElasticsearchMetadataStore(Store):
    """Class to insert data into Elasticsearch instance."""

    def __init__(self, hosts=None):
        super().__init__()
        self.metadata_index = "sdp_meta_data"
        self.hosts = hosts
        self.es_client = None
        self.connect()

    @property
    def es_search_enabled(self):
        """Generic interface to verify there is a Elasticsearch backend"""
        return True

    def connect(self):
        """Connect to Elasticsearch host and create default schema"""
        self.es_client = Elasticsearch(self.hosts)
        if self.es_client.ping():
            # Address the case where the elasticsearch host
            # is no longer reachable
            self.create_schema_if_not_existing(index=self.metadata_index)
            return True
        return False

    def create_schema_if_not_existing(self, index: str):
        """Method to create a Schema from schema and index if it does not yet
        exist."""
        try:
            _ = self.es_client.indices.get(index=index)
        except elasticsearch.NotFoundError:
            with open(
                METADATA_ES_SCHEMA_FILE, "r", encoding="utf-8"
            ) as metadata_schema:
                metadata_schema_json = json.load(metadata_schema)
            self.es_client.indices.create(  # pylint: disable=E1123
                index=index, ignore=400, body=metadata_schema_json
            )

    def clear_metadata_indecise(self):
        """Clear out all indices from elasticsearch instance"""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=self.metadata_index
        )
        self.metadata_list = []

    def insert_metadata(self, metadata_file_json):
        """Method to insert metadata into Elasticsearch."""
        # Add new metadata to es
        result = self.es_client.index(
            index=self.metadata_index, document=metadata_file_json
        )
        return result

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key: str = "*",
        metadata_value: str = "*",
    ):
        """Metadata Search method"""
        if metadata_key != "*" and metadata_value != "*":
            match_criteria = {"match": {metadata_key: metadata_value}}
        else:
            match_criteria = {"match_all": {}}

        query_body = {
            "query": {
                "bool": {
                    "must": [match_criteria],
                    "filter": [
                        {
                            "range": {
                                "date_created": {
                                    "gte": start_date[0:10],
                                    "lte": end_date[0:10],
                                    "format": "yyyy-MM-dd",
                                }
                            }
                        }
                    ],
                }
            }
        }
        if not self.es_client.ping():
            return json.dumps({'Error': 'Elasticsearch unavailable'})

        resp = self.es_client.search(  # pylint: disable=E1123
            index=self.metadata_index, body=query_body
        )
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.add_dataproduct(
                        metadata_file=value,
                        query_key_list=[metadata_key],
                    )
        return json.dumps(self.metadata_list)
