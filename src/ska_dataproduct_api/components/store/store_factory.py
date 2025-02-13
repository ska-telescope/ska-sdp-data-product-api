"""This module contains a function to select the appropriate dataproduct store class."""
import logging
from typing import Union

from ska_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import (
    PGMetadataStore,
    PGSearchStore,
    PostgresConnector,
)
from ska_dataproduct_api.configuration.settings import (
    POSTGRESQL_ANNOTATIONS_TABLE_NAME,
    POSTGRESQL_DBNAME,
    POSTGRESQL_HOST,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_PORT,
    POSTGRESQL_SCHEMA,
    POSTGRESQL_METADATA_TABLE_NAME,
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
            science_metadata_table_name=POSTGRESQL_METADATA_TABLE_NAME,
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
) -> Union[InMemoryDataproductSearch]:
    """
    Selects the appropriate dataproduct search store class.

    """

    try:
        pg_search_store = PGSearchStore(
            db=metadata_store.db,
            science_metadata_table_name=metadata_store.science_metadata_table_name,
            annotations_table_name=metadata_store.annotations_table_name,
        )
        logger.info("PGSearchStore reachable, setting search store to PGSearchStore")
        return pg_search_store
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to connect to PGSearchStore with exception: %s", exception)
        logger.warning("Using InMemoryDataproductSearch search.")
        return InMemoryDataproductSearch(metadata_store)
