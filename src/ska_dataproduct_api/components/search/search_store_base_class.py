"""
This module defines the `MetadataSearchStore` class, which facilitates loading metadata from a
data product store into a search store.
"""

import logging
from typing import Union

from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector

logger = logging.getLogger(__name__)


class MetadataSearchStore:
    """
    A class that facilitates loading metadata from a data product store into a search store.

    Args:
        metadata_store (Union[PostgresConnector, InMemoryVolumeIndexMetadataStore]):
            An instance of either PostgresConnector or InMemoryVolumeIndexMetadataStore
            representing the metadata store from which to load data.
    """

    def __init__(self, metadata_store: Union[PostgresConnector, InMemoryVolumeIndexMetadataStore]):
        self.metadata_store: Union[
            PostgresConnector, InMemoryVolumeIndexMetadataStore
        ] = metadata_store

    def load_metadata_from_store(self) -> None:
        """
        Loads metadata from the configured metadata store into the search store.

        This method delegates the loading logic to the appropriate method based on the
        type of the metadata store.
        """

        if (
            isinstance(self.metadata_store, PostgresConnector)
            and self.metadata_store.postgresql_running
        ):
            self.load_persistent_metadata_store_data()
        else:
            self.load_in_memory_volume_index_metadata_store_data()

    def load_persistent_metadata_store_data(self):
        """
        Loads metadata from a Postgres-based persistent metadata store into the search store.

        Raises:
            NotImplementedError: As this method is intended to be implemented in a derived class.
        """
        for (
            data_product
        ) in self.metadata_store.load_data_products_from_persistent_metadata_store():
            self.insert_metadata_in_search_store(data_product["data"])

    def load_in_memory_volume_index_metadata_store_data(self):
        """
        Loads metadata from an in-memory volume index metadata store into the search store.

        Iterates through the dictionary of data product metadata provided by the in-memory store
        and extracts the metadata dictionary for insertion into the search store.
        """
        for (
            execution_block,
            data_product,
        ) in self.metadata_store.dict_of_data_products_metadata.items():
            print("Loading execution_block %s into search store", execution_block)
            self.insert_metadata_in_search_store(data_product.metadata_dict)

    def insert_metadata_in_search_store(self, metadata_dict: dict) -> dict:
        """
        Inserts the provided metadata dictionary into the search store.

        This method is intended to be implemented in a derived class to handle the specific logic
        of inserting metadata into the chosen search store implementation.

        Args:
            metadata_dict (dict): The dictionary containing the metadata to be inserted.

        Raises:
            NotImplementedError: As this method is intended to be implemented in a derived class.
        """
        raise NotImplementedError(
            "insert_metadata_in_search_store must be implemented in a derived class"
        )
