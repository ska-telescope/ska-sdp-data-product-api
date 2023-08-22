"""Factory to select the correct Store class."""
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.inmemorystore.inmemorystore import (
    InMemoryDataproductIndex,
)


def select_correct_store_class(hosts):
    """Select the store based on elasticsearch availability."""
    es_client = Elasticsearch(hosts=hosts)
    if es_client.ping():
        return ElasticsearchMetadataStore(hosts)
    return InMemoryDataproductIndex()
