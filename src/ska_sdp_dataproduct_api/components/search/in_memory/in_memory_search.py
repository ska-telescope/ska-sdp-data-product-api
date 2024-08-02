"""Module to insert data into Elasticsearch instance."""
import copy
import json
import logging
import pathlib
from time import time
from typing import Any, Dict, List, Union

from ska_sdp_dataproduct_api.components.data_ingestor.data_ingestor import Meta_Data_Ingestor
from ska_sdp_dataproduct_api.components.muidatagrid.mui_datagrid import (
    MuiDataGrid,
    muiDataGridInstance,
)
from ska_sdp_dataproduct_api.components.store.in_memory.in_memory_volume_index_metadata_store import (
    in_memory_volume_index_metadata_store,
)
from ska_sdp_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_sdp_dataproduct_api.configuration.settings import DATE_FORMAT, PERSISTENT_STORAGE_PATH
from ska_sdp_dataproduct_api.utilities.helperfunctions import (
    filter_by_item,
    filter_by_key_value_pair,
    parse_valid_date,
)

logger = logging.getLogger(__name__)

# pylint: disable=no-name-in-module


class InMemoryDataproductSearch:
    """
    This class defines an object that is used to create a list of data products
    based on information contained in the metadata files of these data
    products.
    """

    def __init__(
        self,
        metadata_store: Union[PostgresConnector, in_memory_volume_index_metadata_store],
        muiDataGridInstance: MuiDataGrid,
    ) -> None:
        super().__init__()
        self.mui_data_grid_instance: MuiDataGrid = muiDataGridInstance
        self.metadata_store: Union[
            PostgresConnector, in_memory_volume_index_metadata_store
        ] = metadata_store
        self.number_of_dataproducts: int = 0
        self.load_metadata_from_store()

    def load_metadata_from_store(self):
        """ """
        if self.metadata_store.postgresql_running:
            self.load_persistent_metadata_store_data()
        else:
            self.load_in_memory_volume_index_metadata_store_data()

    def load_in_memory_volume_index_metadata_store_data(self):
        """ """
        for data_product in self.metadata_store.list_of_data_products_metadata:
            self.insert_metadata_in_search_store(data_product)

    def load_persistent_metadata_store_data(self):
        """ """
        for (
            data_product
        ) in self.metadata_store.load_data_products_from_persistent_metadata_store():
            self.insert_metadata_in_search_store(data_product["data"])

    def insert_metadata_in_search_store(self, data_product_metadata_dict: dict):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the flattened_list_of_dataproducts_metadata"""
        # generate a list of keys from this object
        muiDataGridInstance.update_flattened_list_of_keys(data_product_metadata_dict)
        muiDataGridInstance.update_flattened_list_of_dataproducts_metadata(
            muiDataGridInstance.flatten_dict(data_product_metadata_dict)
        )
        self.number_of_dataproducts = self.number_of_dataproducts + 1

    def status(self) -> dict:
        """
        Retrieves the current status of the in-memory data product indexing process.

        This method returns a dictionary containing the following information about in-memory
        data product indexing:

        * `metadata_store_in_use`: Indicates that "InMemoryDataproductSearch" is being used for
        data product metadata storage.
        * `indexing`: A boolean indicating whether data product indexing is currently in progress.
        * `indexing_timestamp` (optional): A timestamp representing when the last data product
        indexing operation started (if available).
        * `number_of_data_products`: The number of data products currently indexed in memory.

        Returns:
            A dictionary containing the current in-memory data product indexing status.
        """

        return {
            "metadata_search_store_in_use": "In memory search store",
            "number_of_dataproducts": self.number_of_dataproducts,
        }

    def sort_metadata_list(self, key: str = "date_created", reverse: bool = True) -> None:
        """Sorts the `flattened_list_of_dataproducts_metadata` attribute of the class instance in-place.

        Args:
            key (str, optional): The key attribute to sort by. Defaults to "date_created".
            reverse (bool, optional): Whether to sort in descending order. Defaults to True.

        Raises:
            TypeError: If the provided `key` is not a string.
            ValueError: If the `key` is not found in the elements of `flattened_list_of_dataproducts_metadata`.
        """

        for element in muiDataGridInstance.flattened_list_of_dataproducts_metadata:
            if key not in element:
                logger.info(
                    "Key %s not found in all elements of flattened_list_of_dataproducts_metadata",
                    key,
                )

        muiDataGridInstance.flattened_list_of_dataproducts_metadata.sort(
            key=lambda x: x[key], reverse=reverse
        )

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key_value_pairs=None,
    ):
        """Metadata Search method."""
        try:
            start_date = parse_valid_date(start_date, DATE_FORMAT)
            end_date = parse_valid_date(end_date, DATE_FORMAT)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Error, invalid time range start_date=%s, end_date %s with error: %s. Using defaults:  \
                    start_date=1970-01-01, end_date 2100-01-01",
                start_date,
                end_date,
                exception,
            )
            start_date: str = "1970-01-01"
            end_date: str = "2100-01-01"

        if metadata_key_value_pairs is None or len(metadata_key_value_pairs) == 0:
            search_results = copy.deepcopy(
                muiDataGridInstance.flattened_list_of_dataproducts_metadata
            )
            for product in muiDataGridInstance.flattened_list_of_dataproducts_metadata:
                try:
                    product_date = parse_valid_date(product["date_created"], DATE_FORMAT)
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.error("Error, invalid date=%s", exception)
                    continue
                if not start_date <= product_date <= end_date:
                    search_results.remove(product)
                    continue

            return json.dumps(search_results)

        search_results = copy.deepcopy(muiDataGridInstance.flattened_list_of_dataproducts_metadata)
        for product in muiDataGridInstance.flattened_list_of_dataproducts_metadata:
            try:
                product_date = parse_valid_date(product["date_created"], DATE_FORMAT)
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.error("Error, invalid date=%s", exception)
                continue
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

    def filter_data(self, mui_data_grid_filter_model:Dict[str, Any], search_panel_options:Dict[str, Any]):
        """This is implemented in subclasses."""
        muiDataGridInstance.load_metadata_from_list(
            muiDataGridInstance.flattened_list_of_dataproducts_metadata
        )

        mui_filtered_data = self.apply_filters(
            muiDataGridInstance.rows.copy(), mui_data_grid_filter_model
        )
        searchbox_filtered_data = self.apply_filters(mui_filtered_data, search_panel_options)

        return searchbox_filtered_data

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
                        except Exception as exception:  # pylint: disable=broad-exception-caught
                            logger.error("Error=%s", exception)
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
