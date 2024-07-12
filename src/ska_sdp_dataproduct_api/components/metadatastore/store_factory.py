"""Factory to select the correct Store class."""
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.components.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.components.inmemorystore.inmemorystore import InMemoryDataproductIndex
from ska_sdp_dataproduct_api.configuration.settings import ES_HOST


def select_correct_store_class():
    """Select the store based on elasticsearch availability."""
    es_client = Elasticsearch(hosts=ES_HOST)
    if es_client.ping():
        return ElasticsearchMetadataStore()
    return InMemoryDataproductIndex()
