"""This module contains the class object used with the MUI DataGrid component in front end
applications"""

import logging
from collections.abc import MutableMapping

# pylint: disable=too-many-instance-attributes
# pylint: disable=too-few-public-methods

logger = logging.getLogger(__name__)


class MuiDataGridColumn:
    """
    Represents a column in a Material-UI Data Grid.

    Attributes:
        width: The width of the column in pixels.
        minWidth: The minimum width of the column in pixels.
        maxWidth: The maximum width of the column in pixels.
        hideable: Whether the column can be hidden by the user.
        sortable: Whether the column can be sorted.
        resizable: Whether the column can be resized by the user.
        filterable: Whether the column can be filtered.
        groupable: Whether the column can be grouped.
        pinnable: Whether the column can be pinned.
        aggregable: Whether the column can be aggregated.
        editable: Whether the column is editable.
        type: The data type of the column.
        align: The alignment of the column content.
        filterOperators: A list of available filter operators for the column.
        field: The field name in the data source.
        headerName: The display name of the column.
        hide: Whether the column is initially hidden.
    """

    # Note: Intentionally matching name to expected field in React, so disabling invalid-name
    def __init__(self, **kwargs):
        self.width = kwargs.get("width", 100)
        self.minWidth = kwargs.get("minWidth", 50)  # pylint: disable=invalid-name
        self.maxWidth = kwargs.get("maxWidth", None)  # pylint: disable=invalid-name
        self.hideable = kwargs.get("hideable", True)
        self.sortable = kwargs.get("sortable", True)
        self.resizable = kwargs.get("resizable", True)
        self.filterable = kwargs.get("filterable", True)
        self.groupable = kwargs.get("groupable", True)
        self.pinnable = kwargs.get("pinnable", True)
        self.aggregable = kwargs.get("aggregable", True)
        self.editable = kwargs.get("editable", False)
        self.type = kwargs.get("type", "string")
        self.align = kwargs.get("align", "left")
        self.filterOperators = kwargs.get(  # pylint: disable=invalid-name
            "filterOperators",
            [
                {"value": "contains"},
                {"value": "equals"},
                {"value": "startsWith"},
                {"value": "endsWith"},
                {"value": "isEmpty", "requiresFilterValue": False},
                {"value": "isNotEmpty", "requiresFilterValue": False},
                {"value": "isAnyOf"},
            ],
        )
        self.field = kwargs.get("field", "default_field")
        self.headerName = kwargs.get(  # pylint: disable=invalid-name
            "headerName", "Default Field Name"
        )  # pylint: disable=invalid-name
        self.hide = kwargs.get("hide", True)

    def basic_column(self) -> dict:
        """
        Returns a basic column configuration for the Data Grid.

        Returns:
            A dictionary representing the basic column configuration.
        """
        return {
            "field": self.field,
            "headerName": self.headerName,
            "width": self.width,
            "hide": self.hide,
        }


class MuiDataGridConfig:
    """Class containing components used with the MUI DataGrid"""

    def __init__(self) -> None:
        self.columns = [
            MuiDataGridColumn(
                field=field, headerName=header_name, width=width, hide=False
            ).basic_column()
            for field, header_name, width in [
                ("execution_block", "Execution Block", 250),
                ("date_created", "Date Created", 150),
                ("config.processing_block", "Processing Block", 250),
                ("config.processing_script", "Processing script", 150),
                ("context.observer", "Observer", 150),
                ("context.intent", "Intent", 150),
                ("context.notes", "Notes", 500),
                ("size", "File size", 80),
                ("status", "Status", 80),
            ]
        ]

        self.table_config: dict = {}
        self.table_config["columns"] = self.columns

        self.flattened_set_of_keys = set()
        self.flattened_set_of_keys.add("annotation")
        self.flattened_list_of_dataproducts_metadata: list[dict] = []

    def update_columns(self, key: str) -> None:
        """
        Updates the columns with a new key if it doesn't exist.

        Args:
            key: The field name of the new column.
        """

        if not any(col.get("field") == key for col in self.columns):
            self.columns.append(
                MuiDataGridColumn(field=key, headerName=key, width=150, hide=False).basic_column()
            )

    def update_flattened_list_of_keys(self, metadata_file: dict) -> None:
        """
        Updates the `flattened_set_of_keys` attribute with new keys extracted from the specified
        metadata file.

        Args:
            metadata_file (dict): The path to the metadata file containing keys to be added.

        Raises:
            TypeError: If `metadata_file` is not a string.
        """
        for key in self.generate_metadata_keys_list(metadata_file, [], "", "."):
            self.flattened_set_of_keys.add(key)
            self.update_columns(key)

    def generate_metadata_keys_list(self, metadata: dict, ignore_keys, parent_key="", sep="."):
        """Given a nested dict, return the flattened list of keys"""
        flattened_list_of_keys = []  # Create an empty list to store flattened keys
        for key, value in metadata.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                flattened_list_of_keys.extend(
                    self.generate_metadata_keys_list(value, ignore_keys, new_key, sep=sep)
                )
            else:
                if new_key not in ignore_keys and new_key not in flattened_list_of_keys:
                    flattened_list_of_keys.append(new_key)
        return flattened_list_of_keys  # Return the flattened list at the end

    def flatten_dict(self, data, prefix=""):
        """
        Flattens a nested dictionary, combining nested keys with a "." separator.

        Args:
            data: The dictionary to flatten.
            prefix: An optional prefix to prepend to flattened keys (default "").

        Returns:
            A new dictionary with flattened keys.
        """
        result = {}
        for key, value in data.items():
            new_key = prefix + key if prefix else key
            if isinstance(value, dict):
                result.update(self.flatten_dict(value, new_key + "."))
            elif value is not None:  # Check if value is not None
                result[new_key] = value
        return result

    def update_flattened_list_of_dataproducts_metadata(self, data_product_details: dict) -> None:
        """
        Updates the internal list of data products with the provided metadata, ensuring
        no duplicates based on `uuid`. If a duplicate is found, it updates the existing
        dictionary with the new values.

        This method adds the provided `data_product_details` dictionary to the internal
        `metadata_list` attribute. If the list is empty, it assigns an "id" of 1 to the
        first data product. Otherwise, it assigns an "id" based on the current length
        of the list + 1.

        Args:
            data_product_details: A dictionary containing the metadata for a data product.

        Returns:
            None
        """
        if "uuid" not in data_product_details:
            return

        for item in self.flattened_list_of_dataproducts_metadata:
            if item["uuid"] == data_product_details["uuid"]:
                # Update the existing dictionary with new values
                item.update(data_product_details)
                return

        # If no duplicate found, add the new dictionary
        if len(self.flattened_list_of_dataproducts_metadata) == 0:
            data_product_details["id"] = 1
        else:
            data_product_details["id"] = len(self.flattened_list_of_dataproducts_metadata) + 1

        self.flattened_list_of_dataproducts_metadata.append(data_product_details)


mui_data_grid_config_instance = MuiDataGridConfig()
