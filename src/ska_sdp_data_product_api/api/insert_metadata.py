"""Module to insert data into Elasticsearch instance."""
import json

import elasticsearch
from elasticsearch import Elasticsearch


class ElasticsearchMetadataStore:
    """Class to insert data into Elasticsearch instance."""

    def __init__(self, hosts):
        self.metadata_index = "sdp_meta_data"
        self.es_client = Elasticsearch(hosts=hosts)
        self.create_schema_if_not_existing(index=self.metadata_index)

    def create_schema_if_not_existing(self, index: str):
        """Method to create a Schema from schema and index if it does not yet
        exist."""
        try:
            _ = self.es_client.indices.get(index=index)
        except elasticsearch.NotFoundError:
            metadata_schema = open(
                "./src/ska_sdp_data_product_api/api/example_schema.json",
                encoding="utf-8",
            )  # TODO Move to settings
            metadata_schema_json = json.load(metadata_schema)
            self.es_client.indices.create(
                index=index, ignore=400, body=metadata_schema_json
            )
            print("No index found for schema, creating new: %s", index)

    def clear_indecise(self):
        """Clear ou all indices from elasticsearch instance"""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=self.metadata_index
        )

    def insert_metadata(
        self,
        metadata_file_path: str,
        date: str,
        dataproduct_file_path: str,
        metadata_file_json,
    ):
        # TODO Tests for file metadata_file exist
        # TODO Tests if data is already in elastic search, if it is, update
        # data
        # TODO Insert needed metadata fields for dashbaord (Date and
        # dataproduct_file_path)
        """Method to insert metadata into Elasticsearch."""
        # Add new metadata to es
        result = self.es_client.index(
            index=self.metadata_index, document=metadata_file_json
        )
        print("result: %s", result)
        return result

    def search_metadata(
        self,
        start_date: str = "20200101",
        end_date: str = "21000101",
        key: str = "*",
        value: str = "*",
    ):
        """Metadata Search method"""
        # query_body = {
        # "query": {
        #     "simple_query_string" : {
        #         "fields": [ key ],
        #         "query": value,
        #     }
        # }
        # }

        query_body = {
            "query": {"query_string": {"query": value, "fields": [key], "fuzziness": 0}}
        }

        resp = self.es_client.search(
            index=self.metadata_index, body=query_body
        )

        metadata_list = []
        id = 1
        all_hits = resp["hits"]["hits"]
        for num, doc in enumerate(all_hits):
            # print ("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")
            for key, value in doc.items():
                if key == "_source":
                    update_dataproduct_list(
                        metadata_list=metadata_list, metadata_file=value, id=id
                    )
                    id = id + 1

        print("Got %d Hits:" % resp["hits"]["total"]["value"])
        metadata_json = json.dumps(metadata_list)
        return metadata_json


def update_dataproduct_list(metadata_list: list, metadata_file: str, id: int):
    """Polulate a list of data products and its metadata"""
    data_product_details = {}
    data_product_details["id"] = id
    for key, value in metadata_file.items():
        if key in (
            "interface",
            "execution_block",
            "date_created",
            "dataproduct_file",
            "metadata_file",
        ):
            data_product_details[key] = value

    metadata_list.append(data_product_details)
