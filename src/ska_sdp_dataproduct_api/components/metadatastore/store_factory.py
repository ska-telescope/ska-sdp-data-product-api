"""This module contains a function to select the appropriate dataproduct store class based on
Elasticsearch availability."""
import logging
from typing import Union

from ska_sdp_dataproduct_api.components.elasticsearch.elasticsearch_api import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.components.inmemorystore.inmemorystore import InMemoryDataproductIndex

logger = logging.getLogger(__name__)


def select_correct_store_class() -> Union[ElasticsearchMetadataStore, InMemoryDataproductIndex]:
    """
    Selects the appropriate dataproduct store class based on Elasticsearch availability.

    This function attempts to connect to Elasticsearch. If the connection is successful,
    an instance of `ElasticsearchMetadataStore` is returned. Otherwise, a warning message
    is logged and an instance of `InMemoryDataproductIndex` is returned for in-memory storage.

    Returns:
        Union[ElasticsearchMetadataStore, InMemoryDataproductIndex]: An instance of either
            `ElasticsearchMetadataStore` or `InMemoryDataproductIndex` depending on Elasticsearch
            availability.
    """

    try:
        elastic_store_instance = ElasticsearchMetadataStore()
        if elastic_store_instance.host and elastic_store_instance.check_and_reconnect():
            logger.info("Elasticsearch reachable, setting search store to ElasticSearch")
            return elastic_store_instance
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to connect to Elasticsearch with exception: %s", exception)
        logger.warning("Using in-memory store.")
        return InMemoryDataproductIndex()

    logger.warning("Elasticsearch not available, setting search store to in-memory store.")
    return InMemoryDataproductIndex()
