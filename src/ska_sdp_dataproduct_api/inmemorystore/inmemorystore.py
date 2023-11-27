"""Module to insert data into Elasticsearch instance."""
import json
import logging
import time
from collections.abc import MutableMapping

from ska_sdp_dataproduct_api.core.settings import DATE_FORMAT
from ska_sdp_dataproduct_api.metadatastore.datastore import Store
from ska_sdp_dataproduct_api.core.helperfunctions import check_date_format

logger = logging.getLogger(__name__)

# pylint: disable=no-name-in-module


class InMemoryDataproductIndex(Store):
    """
    This class defines an object that is used to create a list of data products
    based on information contained in the metadata files of these data
    products.
    """

    def __init__(self) -> None:
        super().__init__()
        self.reindex()

    @property
    def es_search_enabled(self):
        """Generic interface to verify there is no Elasticsearch backend"""
        return False

    def clear_metadata_indecise(self):
        """Clear out all indices from in memory instance"""
        self.metadata_list.clear()

    def insert_metadata(self, metadata_file_json):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the metadata_list"""
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = self.generate_metadata_keys_list(
            metadata_file, ["files"], "", "."
        )

        self.add_dataproduct(
            metadata_file=metadata_file,
            query_key_list=query_key_list,
        )

    def generate_metadata_keys_list(
        self, metadata, ignore_keys, parent_key="", sep="_"
    ):
        """Given a nested dict, return the flattened list of keys"""
        items = []
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(
                    self.generate_metadata_keys_list(
                        value, ignore_keys, new_key, sep=sep
                    )
                )
            else:
                if new_key not in ignore_keys:
                    items.append(new_key)
        return items

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key: str = "*",
        metadata_value: str = "*",
    ):
        """Metadata Search method"""

        start_date = check_date_format(start_date, DATE_FORMAT)
        end_date = check_date_format(end_date, DATE_FORMAT)

        search_results = []
        for product in self.metadata_list:
            product_date = time.strptime(product["date_created"], DATE_FORMAT)
            if not start_date <= product_date <= end_date:
                continue
            if metadata_key == "*" and metadata_value == "*":
                search_results.append(product)
                continue
            try:
                product_value = product[metadata_key]
                if product_value == metadata_value:
                    search_results.append(product)
            except KeyError:
                continue
        return json.dumps(search_results)
