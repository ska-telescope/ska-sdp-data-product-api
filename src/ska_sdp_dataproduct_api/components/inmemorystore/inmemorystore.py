"""Module to insert data into Elasticsearch instance."""
import copy
import json
import logging
from collections.abc import MutableMapping
from typing import Any, Dict, List

from ska_sdp_dataproduct_api.components.metadatastore.datastore import Store
from ska_sdp_dataproduct_api.configuration.settings import DATE_FORMAT
from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    filter_by_item,
    filter_by_key_value_pair,
    parse_valid_date,
)

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
        self.number_of_dataproducts: int

    def status(self) -> dict:
        """
        Retrieves the current status of the in-memory data product indexing process.

        This method returns a dictionary containing the following information about in-memory
        data product indexing:

        * `metadata_store_in_use`: Indicates that "InMemoryDataproductIndex" is being used for
        data product metadata storage.
        * `indexing`: A boolean indicating whether data product indexing is currently in progress.
        * `indexing_timestamp` (optional): A timestamp representing when the last data product
        indexing operation started (if available).
        * `number_of_data_products`: The number of data products currently indexed in memory.

        Returns:
            A dictionary containing the current in-memory data product indexing status.
        """

        return {
            "metadata_store_in_use": "InMemoryDataproductIndex",
            "indexing": self.indexing,
            "indexing_timestamp": self.indexing_timestamp,  # Optional
            "number_of_data_products": self.number_of_dataproducts,
        }

    def clear_metadata_indecise(self):
        """Clear out all indices from in memory instance"""
        self.metadata_list.clear()
        self.number_of_dataproducts = 0

    def insert_metadata(self, metadata_file_json):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the metadata_list"""
        # load JSON into object
        metadata_file = json.loads(metadata_file_json)

        # generate a list of keys from this object
        query_key_list = self.generate_metadata_keys_list(metadata_file, [], "", ".")

        self.add_dataproduct(
            metadata_file=metadata_file,
            query_key_list=query_key_list,
        )

    def generate_metadata_keys_list(self, metadata, ignore_keys, parent_key="", sep="_"):
        """Given a nested dict, return the flattened list of keys"""
        items = []
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(
                    self.generate_metadata_keys_list(value, ignore_keys, new_key, sep=sep)
                )
            else:
                if new_key not in ignore_keys:
                    items.append(new_key)
        return items

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key_value_pairs=None,
    ):
        """Metadata Search method."""
        start_date = parse_valid_date(start_date, DATE_FORMAT)
        end_date = parse_valid_date(end_date, DATE_FORMAT)

        if metadata_key_value_pairs is None or len(metadata_key_value_pairs) == 0:
            search_results = copy.deepcopy(self.metadata_list)
            for product in self.metadata_list:
                product_date = parse_valid_date(product["date_created"], DATE_FORMAT)
                if not start_date <= product_date <= end_date:
                    search_results.remove(product)
                    continue

            return json.dumps(search_results)

        search_results = copy.deepcopy(self.metadata_list)
        for product in self.metadata_list:
            product_date = parse_valid_date(product["date_created"], DATE_FORMAT)

            if not start_date <= product_date <= end_date:
                search_results.remove(product)
                continue
            for key_value_pair in metadata_key_value_pairs:
                if (
                    key_value_pair["metadata_key"] == "*"
                    and key_value_pair["metadata_value"] == "*"
                ):
                    continue
                try:
                    product_value = product[key_value_pair["metadata_key"]]
                    if product_value != key_value_pair["metadata_value"]:
                        search_results.remove(product)
                except KeyError:
                    continue
        return json.dumps(search_results)

    def apply_filters(
        self, data: List[Dict[str, Any]], filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Filters a list of dictionaries based on a provided set of filter criteria.

        Args:
            data: The list of dictionaries to filter.
            filters: A dictionary containing filter criteria. This dictionary should have the
            following keys:
                * logicOperator (optional, defaults to "and"): The logical operator to use when
                combining multiple filters (e.g., "and", "or").
                * items: A list of dictionaries representing individual filter items. Each filter
                item dictionary should have the following keys:
                    * field: The field name to filter on.
                    * operator: The filtering operation to perform (e.g., "contains", "equals",
                    "startsWith", "endsWith", "isAnyOf").
                    * value: The value to compare with the field.

        Returns:
            A new list containing only the dictionaries that match all filters (for "and") or at
            least one filter (for "or").

        Raises:
            ValueError: If a filter item is missing required fields ("field", "operator", or
            "value").
        """

        # logic_operator = filters.get("logicOperator", "and").lower()
        filtered_data = data

        for filter_item in filters.get("items", []):
            field = filter_item.get("field")
            comparator = filter_item.get("value")
            operator = filter_item.get("operator")
            key_pairs = filter_item.get("keyPairs")

            if field and operator and comparator:
                match field:
                    case "date_created":
                        try:
                            filtered_data = filter_by_item(
                                filtered_data,
                                field,
                                operator,
                                parse_valid_date(comparator, "%Y-%m-%d"),
                            )
                        except ValueError:
                            continue
                    case _:
                        try:
                            filtered_data = filter_by_item(
                                filtered_data, field, operator, comparator
                            )
                        except ValueError:
                            continue

            if field and key_pairs:
                match field:
                    case "formFields":
                        try:
                            filtered_data = filter_by_key_value_pair(filtered_data, key_pairs)
                        except ValueError:
                            continue

            # Implement logic based on logicOperator (and or or)

        return filtered_data
