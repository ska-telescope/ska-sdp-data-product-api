"""MetadataStore module

This module provides a `MetadataStore` class to manage and update metadata associated with data
products.

The `MetadataStore` class offers functionalities to:

- Store timestamps for indexing and data modification.
- Update the date modified timestamp when data is added or modified.

"""

import datetime
import logging
from time import time

logger = logging.getLogger(__name__)

# pylint: disable=too-few-public-methods


class MetadataStore:
    """
    This class contain methods common to the InMemoryVolumeIndexMetadataStore and the PostgreSQL
    store
    """

    def __init__(self):
        self.indexing_timestamp: time = time()
        self.indexing: bool = False
        self.date_modified = datetime.datetime.now()

    def update_data_store_date_modified(self):
        """This method updates the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()
