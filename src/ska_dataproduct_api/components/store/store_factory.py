"""This module contains a function to select the appropriate dataproduct store class based on
Elasticsearch availability."""
import logging
from typing import Union

from ska_dataproduct_api.components.search.elasticsearch.elasticsearch import (
    ElasticsearchMetadataStore,
)
from ska_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import (
    PGMetadataStore,
    PostgresConnector,
)
from ska_dataproduct_api.configuration.settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_INDICES,
    ELASTICSEARCH_METADATA_SCHEMA_FILE,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_USER,
    POSTGRESQL_ANNOTATIONS_TABLE_NAME,
    POSTGRESQL_DBNAME,
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_SCHEMA,
    POSTGRESQL_TABLE_NAME,
    POSTGRESQL_USER,
)

logger = logging.getLogger(__name__)


def select_metadata_store_class() -> Union[PGMetadataStore, InMemoryVolumeIndexMetadataStore]:
    """
    Selects the appropriate dataproduct metadata store class based on PostgreSQL availability.

    This function attempts to connect to PostgreSQL. If the connection is successful,
    an instance of `PostgresConnector` is returned. Otherwise, a warning message
    is logged and an instance of `InMemoryVolumeIndexMetadataStore` is returned for in-memory
    storage.

    Returns:
        Union[PostgresConnector, InMemoryVolumeIndexMetadataStore]: An instance of either
            `PostgresConnector` or `InMemoryVolumeIndexMetadataStore` depending on PostgreSQL
            availability.
    """

    try:
        metadata_db = PostgresConnector(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_PORT,
            user=POSTGRESQL_USER,
            schema=POSTGRESQL_SCHEMA,
            password=POSTGRESQL_PASSWORD,
            dbname=POSTGRESQL_DBNAME,
        )

        persistent_metadata_store = PGMetadataStore(
            db=metadata_db,
            science_metadata_table_name=POSTGRESQL_TABLE_NAME,
            annotations_table_name=POSTGRESQL_ANNOTATIONS_TABLE_NAME,
        )
        logger.info("PostgreSQL reachable, setting metadata store to obtain data from PostgreSQL")
        return persistent_metadata_store
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.exception("Failed to connect to PostgreSQL with error: %s", exception)
        logger.warning("Using InMemoryVolumeIndexMetadataStore store.")
        return InMemoryVolumeIndexMetadataStore()


def select_search_store_class(
    metadata_store: Union[PGMetadataStore, InMemoryVolumeIndexMetadataStore],
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
    elastic_store_instance = ElasticsearchMetadataStore(
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT,
        user=ELASTICSEARCH_USER,
        password=ELASTICSEARCH_PASSWORD,
        indices=ELASTICSEARCH_INDICES,
        schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
        metadata_store=metadata_store,
    )
    elastic_store_instance.check_and_reconnect()

    try:
        elastic_store_instance = ElasticsearchMetadataStore(
            host=ELASTICSEARCH_HOST,
            port=ELASTICSEARCH_PORT,
            user=ELASTICSEARCH_USER,
            password=ELASTICSEARCH_PASSWORD,
            indices=ELASTICSEARCH_INDICES,
            schema=ELASTICSEARCH_METADATA_SCHEMA_FILE,
            metadata_store=metadata_store,
        )
        if elastic_store_instance.host and elastic_store_instance.check_and_reconnect():
            logger.info("Elasticsearch reachable, setting search store to ElasticSearch")
            return elastic_store_instance
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to connect to Elasticsearch with exception: %s", exception)
        logger.warning("Using InMemoryDataproductSearch search.")
        return InMemoryDataproductSearch(metadata_store)

    logger.warning(
        "Elasticsearch not available, setting search store to InMemoryDataproductSearch\
                    store."
    )
    return InMemoryDataproductSearch(metadata_store)
