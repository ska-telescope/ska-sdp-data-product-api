"""Module to insert data into Elasticsearch instance."""
import json

import elasticsearch
from elasticsearch import Elasticsearch

from ska_sdp_data_product_api.core.settings import METADATA_ES_SCHEMA_FILE


class ElasticsearchMetadataStore:
    """Class to insert data into Elasticsearch instance."""

    def __init__(self):
        self.metadata_index = "sdp_meta_data"
        self.metadata_list = []
        self.metadata_list_id = 1
        self.es_client = None
        self.es_search_enabled = True

    def connect(self, hosts):
        """Connect to Elasticsearch host and create default schema"""
        try:
            self.es_client = Elasticsearch(hosts=hosts)
            self.create_schema_if_not_existing(index=self.metadata_index)
        except elasticsearch.exceptions.ConnectionError:
            # If now connection is available, disable search.
            self.es_search_enabled = False

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

    def clear_indecise(self):
        """Clear ou all indices from elasticsearch instance"""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=self.metadata_index
        )

    def insert_metadata(
        self,
        metadata_file_json,
    ):
        """Method to insert metadata into Elasticsearch."""
        # Add new metadata to es
        result = self.es_client.index(
            index=self.metadata_index, document=metadata_file_json
        )
        return result

    def list_all_dataproducts(self):
        """When search is not avialable, this endpoint will return all the
        dataproducts so it can be listed in the table on the dashboard."""
        return json.dumps(self.metadata_list)

    def search_metadata(
        self,
        start_date: str = "2020-01-01",
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
        resp = self.es_client.search(  # pylint: disable=E1123
            index=self.metadata_index, body=query_body
        )
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.update_dataproduct_list(
                        metadata_file=value,
                    )
        return json.dumps(self.metadata_list)

    def update_dataproduct_list(self, metadata_file: str):
        """Polulate a list of data products and its metadata"""
        data_product_details = {}
        data_product_details["id"] = self.metadata_list_id
        for key, value in metadata_file.items():
            if key in (
                "interface",
                "execution_block",
                "date_created",
                "dataproduct_file",
                "metadata_file",
            ):
                data_product_details[key] = value
        self.metadata_list.append(data_product_details)
        self.metadata_list_id = self.metadata_list_id + 1
