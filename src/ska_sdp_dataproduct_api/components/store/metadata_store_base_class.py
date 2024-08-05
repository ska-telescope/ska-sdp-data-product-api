import datetime
import logging
from time import time

logger = logging.getLogger(__name__)


class MetadataStore:
    def __init__(self):
        self.indexing_timestamp: time = time()
        self.indexing: bool = False
        pass

    def update_data_store_date_modified(self):
        """This method updates the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()
