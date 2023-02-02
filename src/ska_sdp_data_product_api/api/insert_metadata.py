"""Module to insert data into Elasticsearch instance."""
import json

from elasticsearch import Elasticsearch
import elasticsearch

class ElasticsearchMetadataStore:
    """Class to insert data into Elasticsearch instance."""
    def __init__(self, hosts):
        self.metadata_index = 'sdp_meta_data'
        self.es_client = Elasticsearch(hosts=hosts)
        self.create_schema_if_not_existing(index=self.metadata_index)
    
    def create_schema_if_not_existing(self, index: str):
        """Method to create a Schema from schema and index if it does not yet exsit."""
        try:
            _ = self.es_client.indices.get(index=index)
        except elasticsearch.NotFoundError:
            metadata_schema = open('./src/ska_sdp_data_product_api/api/example_schema.json') # TODO Move to settings
            metadata_schema_json = json.load(metadata_schema)
            self.es_client.indices.create(index=index, ignore=400, body=metadata_schema_json)
            print("No index found for schema, creating new: %s", index)

    def insert_metadata(self, metadata_file: str):
        # TODO Tests for file metadata_file exist
        # TODO Tests if data is already in elastic search, if it is, update data 
        """Method to insert metadata into Elasticsearch."""
        self.es_client.index(index=self.metadata_index, document=json.load(open(metadata_file)))
   
    def search_metadata(self, start_date: str = "20200101",end_date: str = "21000101",simple_query_string: str = "*"):
        ## simple_query_string
        query_body = {
        "query": {
            "simple_query_string" : {
                "query": simple_query_string,
            }
        }
        }

        resp = self.es_client.search(index=self.metadata_index, body=query_body)

        metadata_list = []
        id = 1
        all_hits = resp['hits']['hits']
        for num, doc in enumerate(all_hits):
            # print ("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")

            for key, value in doc.items():
                if key == "_source":
                    update_dataproduct_list(metadata_list=metadata_list, metadata_file=value, id=id)
                    id = id + 1
            print ("\n\n")

        print("Got %d Hits:" % resp['hits']['total']['value'])
        metadata_json = json.dumps(metadata_list)
        return metadata_json

def update_dataproduct_list(metadata_list : list, metadata_file: str, id: int):
    """Polulate a list of data products and its metadata"""
    data_product_details = {}
    data_product_details["id"] = id
    for key, value in metadata_file.items():
        print(key, value)
        if key == "interface":
            data_product_details[key] = value
            # data_product_details.append(key : value)
        if key == "execution_block":
            data_product_details[key] = value
            # data_product_details.append(key : value)

    metadata_list.append(data_product_details)
    
