"""Factory to select the correct Store class."""
from elasticsearch import Elasticsearch

from ska_sdp_dataproduct_api.components.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.components.inmemorystore.inmemorystore import InMemoryDataproductIndex
from ska_sdp_dataproduct_api.utilities.helperfunctions import DPDAPIStatus


def select_correct_store_class(hosts, dpd_api_status: DPDAPIStatus):
    """Select the store based on elasticsearch availability."""
    es_client = Elasticsearch(hosts=hosts)
    if es_client.ping():
        return ElasticsearchMetadataStore(dpd_api_status, hosts)
    return InMemoryDataproductIndex(dpd_api_status)
