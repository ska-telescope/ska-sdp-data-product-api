"""Module to insert data into Elasticsearch instance."""
import copy
import datetime
import json
import logging
from typing import Any, Union

from ska_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance
from ska_dataproduct_api.components.search.search_store_base_class import MetadataSearchStore
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_dataproduct_api.configuration.settings import DATE_FORMAT
from ska_dataproduct_api.utilities.helperfunctions import (
    filter_by_item,
    filter_by_key_value_pair,
    parse_valid_date,
)

logger = logging.getLogger(__name__)

# pylint: disable=no-name-in-module


class InMemoryDataproductSearch(MetadataSearchStore):
    """
    This class defines an object that is used to create a list of data products
    based on information contained in the metadata files of these data
    products.
    """

    def __init__(
        self,
        metadata_store: Union[PostgresConnector, InMemoryVolumeIndexMetadataStore],
    ) -> None:
        super().__init__(metadata_store)
        muiDataGridInstance.flattened_set_of_keys.clear()
        muiDataGridInstance.flattened_list_of_dataproducts_metadata.clear()
        self.load_metadata_from_store()

    def insert_metadata_in_search_store(self, metadata_dict: dict):
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the flattened_list_of_dataproducts_metadata"""
        # generate a list of keys from this object
        muiDataGridInstance.update_flattened_list_of_keys(metadata_dict)
        muiDataGridInstance.update_flattened_list_of_dataproducts_metadata(
            muiDataGridInstance.flatten_dict(metadata_dict)
        )

        self.sort_list_of_dict(
            list_of_dict=muiDataGridInstance.flattened_list_of_dataproducts_metadata
        )

        self.number_of_dataproducts = self.number_of_dataproducts + 1

    def sort_list_of_dict(
        self, list_of_dict: list[dict], key: str = "date_created", reverse: bool = True
    ) -> None:
        """Sorts the list_of_dict instance in-place.

        Args:
            list_of_dict (list[dict]): The list of dictionaries to sort.
            key (str, optional): The key attribute to sort by. Defaults to "date_created".
            reverse (bool, optional): Whether to sort in descending order. Defaults to True.

        Raises:
            None
        """
        list_of_dict.sort(key=lambda x: x.get(key), reverse=reverse)

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

    def search_metadata(
        self,
        start_date: str = "1970-01-01",
        end_date: str = "2100-01-01",
        metadata_key_value_pairs=None,
    ):
        """Metadata Search method."""
        try:
            start_date_datetime = parse_valid_date(start_date, DATE_FORMAT)
            end_date_datetime = parse_valid_date(end_date, DATE_FORMAT)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(
                "Error, invalid time range start_date=%s, end_date=%s with error: %s. \
                    Using defaults: start_date=1970-01-01, end_date 2100-01-01",
                start_date,
                end_date,
                exception,
            )
            start_date_datetime: datetime.datetime = parse_valid_date("1970-01-01", DATE_FORMAT)
            end_date_datetime: datetime.datetime = parse_valid_date("2100-01-01", DATE_FORMAT)

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
                if not start_date_datetime <= product_date <= end_date_datetime:
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
            if not start_date_datetime <= product_date <= end_date_datetime:
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

    def filter_data(
        self,
        mui_data_grid_filter_model: dict[str, Any],
        search_panel_options: dict[str, Any],
        users_user_group_list: list[str],
    ) -> list[dict]:
        """Filters data based on provided criteria.

        Args:
            mui_data_grid_filter_model: Filter model from the MUI data grid.
            search_panel_options: Search panel options.
            users_user_group_list: List of user groups.

        Returns:
            Filtered data.
        """
        muiDataGridInstance.load_metadata_from_list(
            muiDataGridInstance.flattened_list_of_dataproducts_metadata
        )

        access_filtered_data = self.access_filter(
            data=muiDataGridInstance.rows.copy(), users_user_groups=users_user_group_list
        )
        mui_filtered_data = self.apply_filters(access_filtered_data, mui_data_grid_filter_model)
        searchbox_filtered_data = self.apply_filters(mui_filtered_data, search_panel_options)

        return searchbox_filtered_data

    def access_filter(
        self, data: list[dict[str, Any]], users_user_groups: list[str]
    ) -> list[dict[str, Any]]:
        """Filters the mui_data_grid_filter_model based on access groups.

        Args:
            data: A list of dictionaries representing filter model data.
            users_user_groups: A list of user group names.

        Returns:
            A filtered list of dictionaries where either no access_group is assigned or the
            assigned access_group is in the users_user_groups list.
        """
        filtered_model = []
        for item in data:
            access_group = item.get("context.access_group", None)
            if access_group is None or access_group in users_user_groups:
                filtered_model.append(item)
        return filtered_model

    def apply_filters(
        self, data: list[dict[str, Any]], filters: dict[str, Any]
    ) -> list[dict[str, Any]]:
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
