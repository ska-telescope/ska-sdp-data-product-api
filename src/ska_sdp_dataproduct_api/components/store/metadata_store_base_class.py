import datetime
import logging

logger = logging.getLogger(__name__)


class MetadataStore:
    def __init__(self):
        pass

    def update_data_store_date_modified(self):
        """This method updates the timestamp of the last time that data was
        added or modified in the data product store by this API"""
        self.date_modified = datetime.datetime.now()
