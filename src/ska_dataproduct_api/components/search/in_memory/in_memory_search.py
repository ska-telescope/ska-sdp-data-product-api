"""Module contains methods to search through data products in memory."""
import logging
from typing import Any, Union

from ska_dataproduct_api.components.muidatagrid.mui_datagrid import mui_data_grid_config_instance
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)
from ska_dataproduct_api.components.store.persistent.postgresql import PostgresConnector
from ska_dataproduct_api.utilities.helperfunctions import (
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
        metadata_store: Union[PostgresConnector, InMemoryVolumeIndexMetadataStore],
    ) -> None:
        self.number_of_dataproducts: int = 0
        self.metadata_store = metadata_store

        mui_data_grid_config_instance.flattened_set_of_keys.clear()
        mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata.clear()

    def insert_data_products_into_muidatagrid(self, metadata_dict: dict) -> None:
        """This method loads the metadata file of a data product, creates a
        list of keys used in it, and then adds it to the flattened_list_of_dataproducts_metadata"""
        # generate a list of keys from this object
        mui_data_grid_config_instance.update_flattened_list_of_keys(metadata_dict)
        mui_data_grid_config_instance.update_flattened_list_of_dataproducts_metadata(
            mui_data_grid_config_instance.flatten_dict(metadata_dict)
        )

        self.sort_list_of_dict(
            list_of_dict=mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata
        )

        self.number_of_dataproducts = len(
            mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata
        )

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
        Retrieves the current status of the in-memory data store.

        * `metadata_search_store_in_use`: Indicates that in memory search store is being used for
        data product metadata storage.
        * `number_of_dataproducts`: The number of data products currently indexed in memory.

        Returns:
            A dictionary containing the current in-memory data product store status.
        """

        return {
            "metadata_search_store_in_use": "In memory search store",
            "number_of_dataproducts": self.number_of_dataproducts,
        }

    def load_in_memory_volume_index_metadata_store_data(self):
        """
        Loads metadata from an in-memory volume index metadata store into the MUI data grid class.
        """
        for (
            data_product_uid,
            data_product,
        ) in self.metadata_store.dict_of_data_products_metadata.items():
            logger.debug("Loading UUID %s into search store", data_product_uid)
            self.insert_data_products_into_muidatagrid(data_product.appended_metadata_dict())

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
        mui_data_rows: list[dict] = []

        try:
            mui_data_grid_filter_model["items"].extend(search_panel_options.get("items", []))
        except KeyError:
            mui_data_grid_filter_model["items"] = search_panel_options.get("items", [])

        self.load_in_memory_volume_index_metadata_store_data()
        for row in mui_data_grid_config_instance.flattened_list_of_dataproducts_metadata:
            mui_data_rows.append(row)

        access_filtered_data = self.access_filter(
            data=mui_data_rows.copy(), users_user_groups=users_user_group_list
        )
        mui_filtered_data = self.apply_filters(access_filtered_data, mui_data_grid_filter_model)

        return mui_filtered_data

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
