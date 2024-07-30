from typing import Union

from ska_sdp_dataproduct_api.components.in_memory_volume_index_metadata_store.in_memory_volume_index_metadata_store import (
    in_memory_volume_index_metadata_store,
)
from ska_sdp_dataproduct_api.components.persistent_metadata_store.postgresql import (
    PostgresConnector,
)


class Meta_Data_Ingestor:
    """Class to handle data ingest from various sources"""

    def __init__(
        self, metadata_store: Union[PostgresConnector, in_memory_volume_index_metadata_store]
    ):
        """ """
        self.metadata_store: Union[
            PostgresConnector, in_memory_volume_index_metadata_store
        ] = metadata_store

    def load_data_products_from_storage(self):
        """ """
        pass
        if self.metadata_store.postgresql_running:
            pass
        else:
            self.metadata_store.load_data_products()
