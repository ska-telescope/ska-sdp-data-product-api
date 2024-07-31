"""This module contains a function to select the appropriate dataproduct store class based on
Elasticsearch availability."""
import logging
from typing import Union

from ska_sdp_dataproduct_api.components.search.elasticsearch.elasticsearch import (
    ElasticsearchMetadataStore,
)
from ska_sdp_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_sdp_dataproduct_api.components.store.in_memory.in_memory_volume_index_metadata_store import (
    in_memory_volume_index_metadata_store,
)
from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import (
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_TABLE_NAME,
    POSTGRESQL_USER,
)

logger = logging.getLogger(__name__)


def select_persistent_metadata_store_class() -> Union[
    PostgresConnector, in_memory_volume_index_metadata_store
]:
    """
    Selects the appropriate dataproduct search store class based on Elasticsearch availability.

    This function attempts to connect to Elasticsearch. If the connection is successful,
    an instance of `ElasticsearchMetadataStore` is returned. Otherwise, a warning message
    is logged and an instance of `InMemoryDataproductSearch` is returned for in-memory storage.

    Returns:
        Union[ElasticsearchMetadataStore, InMemoryDataproductSearch]: An instance of either
            `ElasticsearchMetadataStore` or `InMemoryDataproductSearch` depending on Elasticsearch
            availability.
    """

    try:

        persistent_metadata_store = PostgresConnector(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_PORT,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
            table_name=POSTGRESQL_TABLE_NAME,
        )

        if persistent_metadata_store.postgresql_running:
            logger.info(
                "PostgreSQL reachable, setting metadata store to obtain data from PostgreSQL"
            )
            return persistent_metadata_store
        logger.warning(
            "PostgreSQL not available, loading metadata from persistent volume into in memory store."
        )
        return in_memory_volume_index_metadata_store()
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to connect to Elasticsearch with exception: %s", exception)
        logger.warning("Using in-memory store.")
        return in_memory_volume_index_metadata_store()


def select_correct_search_store_class(
    metadata_store: Union[PostgresConnector, in_memory_volume_index_metadata_store],
    muiDataGridInstance,
) -> Union[ElasticsearchMetadataStore, InMemoryDataproductSearch]:
    """
    Selects the appropriate dataproduct search store class based on Elasticsearch availability.

    This function attempts to connect to Elasticsearch. If the connection is successful,
    an instance of `ElasticsearchMetadataStore` is returned. Otherwise, a warning message
    is logged and an instance of `InMemoryDataproductSearch` is returned for in-memory storage.

    Returns:
        Union[ElasticsearchMetadataStore, InMemoryDataproductSearch]: An instance of either
            `ElasticsearchMetadataStore` or `InMemoryDataproductSearch` depending on Elasticsearch
            availability.
    """
    elastic_store_instance = ElasticsearchMetadataStore(metadata_store)
    elastic_store_instance.check_and_reconnect()

    try:
        elastic_store_instance = ElasticsearchMetadataStore(metadata_store)
        if elastic_store_instance.host and elastic_store_instance.check_and_reconnect():
            logger.info("Elasticsearch reachable, setting search store to ElasticSearch")
            return elastic_store_instance
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to connect to Elasticsearch with exception: %s", exception)
        logger.warning("Using in-memory search.")
        return InMemoryDataproductSearch(metadata_store, muiDataGridInstance)

    logger.warning("Elasticsearch not available, setting search store to in-memory store.")
    return InMemoryDataproductSearch(metadata_store, muiDataGridInstance)
