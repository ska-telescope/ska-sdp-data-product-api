"""Module to insert data into Elasticsearch instance."""
import json

import elasticsearch
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.core.settings import METADATA_ES_SCHEMA_FILE


class ElasticsearchMetadataStore:
    """Class to insert data into Elasticsearch instance."""

    def __init__(self):
        self.metadata_index = "sdp_meta_data"
        self.metadata_list = []
        self.es_client = None
        self.es_search_enabled = True

    def connect(self, hosts):
        """Connect to Elasticsearch host and create default schema"""
        try:
            self.es_client = Elasticsearch(hosts=hosts)
            self.create_schema_if_not_existing(index=self.metadata_index)
            self.es_search_enabled = True
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
        """Clear out all indices from elasticsearch instance"""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=self.metadata_index
        )
        self.metadata_list = []

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
        """When search is not available, this endpoint will return all the
        dataproducts so it can be listed in the table on the dashboard."""
        return json.dumps(self.metadata_list)

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
        try:
            resp = self.es_client.search(  # pylint: disable=E1123
                index=self.metadata_index, body=query_body
            )
        except elasticsearch.exceptions.ConnectionError:
            self.es_search_enabled = False
        all_hits = resp["hits"]["hits"]
        self.metadata_list = []
        for _num, doc in enumerate(all_hits):
            for key, value in doc.items():
                if key == "_source":
                    self.update_dataproduct_list(
                        metadata_file=value,
                        query_key_list=[metadata_key] # at present users can only query using a single metadata_key, but update_dataproduct_list supports many query keys
                    )
        return json.dumps(self.metadata_list)

    def update_dataproduct_list(self, metadata_file: str, query_key_list):
        """Populate a list of data products and its metadata"""
        data_product_details = {}
        data_product_details["id"] = len(self.metadata_list) + 1
        for key, value in metadata_file.items():
            if key in (
                "interface",
                "execution_block",
                "date_created",
                "dataproduct_file",
                "metadata_file",
            ):
                data_product_details[key] = value

        # add additional keys based on the query
        for query_key in query_key_list:
            query_metadata = self.find_metadata(metadata_file, query_key)
            if query_metadata is not None:
                data_product_details[query_metadata['key']] = query_metadata['value']

        self.metadata_list.append(data_product_details)


    def find_metadata(self, metadata, query_key):
        """ Given a dict of metadata, and a period-separated hierarchy of keys,
            return the key and the value found within the dict.
            For example: Given a dict and the key a.b.c,
            return the key (a.b.c) and the value dict[a][b][c] """
        keys = query_key.split('.')

        subsection = metadata
        for key in keys:
            if key in subsection:
                subsection = subsection[key]
            else:
                return None

        return {'key': query_key, 'value': subsection}