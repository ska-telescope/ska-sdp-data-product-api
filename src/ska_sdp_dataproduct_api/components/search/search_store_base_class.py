import logging
from typing import Union

from ska_sdp_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector

logger = logging.getLogger(__name__)


class MetadataSearchStore:
    def __init__(self, metadata_store: Union[PostgresConnector, InMemoryVolumeIndexMetadataStore]):
        self.metadata_store: Union[
            PostgresConnector, InMemoryVolumeIndexMetadataStore
        ] = metadata_store

    def load_metadata_from_store(self):
        """ """
        if self.metadata_store.postgresql_running:
            self.load_persistent_metadata_store_data()
        else:
            self.load_in_memory_volume_index_metadata_store_data()

    def load_persistent_metadata_store_data(self):
        """ """
        for (
            data_product
        ) in self.metadata_store.load_data_products_from_persistent_metadata_store():
            self.insert_metadata_in_search_store(data_product["data"])

    def load_in_memory_volume_index_metadata_store_data(self):
        """Loads metadata from the metadata store into the search store."""
        for (
            execution_block,
            data_product,
        ) in self.metadata_store.dict_of_data_products_metadata.items():
            print("Loading execution_block %s into search store", execution_block)
            self.insert_metadata_in_search_store(data_product.metadata_dict)

    def insert_metadata_in_search_store(self, metadata_dict: dict) -> dict:
        """Defined in derived class"""
        pass
