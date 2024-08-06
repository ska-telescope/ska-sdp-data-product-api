"""This module contains the class object used with the MUI DataGrid component in front end
applications"""
from collections.abc import MutableMapping

# pylint: disable=too-many-instance-attributes


class MuiDataGrid:
    """Class containing components used with the MUI DataGrid"""

    def __init__(self) -> None:

        self.default_column: dict = {
            "width": 100,
            "minWidth": 50,
            "maxWidth": None,  # None represents null in Python
            "hideable": True,
            "sortable": True,
            "resizable": True,
            "filterable": True,
            "groupable": True,
            "pinnable": True,
            "aggregable": True,
            "editable": False,
            "type": "string",
            "align": "left",
            "filterOperators": [
                {"value": "contains"},
                {"value": "equals"},
                {"value": "startsWith"},
                {"value": "endsWith"},
                {"value": "isEmpty", "requiresFilterValue": False},
                {"value": "isNotEmpty", "requiresFilterValue": False},
                {"value": "isAnyOf"},
            ],
            "field": "default_field",
            "headerName": "Default Field Name",
            "hide": True,
        }

        self.initial_column_config: list[dict] = [
            {
                "field": "execution_block",
                "headerName": "Execution Block",
                "width": 250,
                "hide": False,
            },
            {"field": "date_created", "headerName": "Date Created", "width": 150, "hide": False},
            {"field": "context.observer", "headerName": "Observer", "width": 150, "hide": False},
            {
                "field": "config.processing_block",
                "headerName": "Processing Block",
                "width": 250,
                "hide": False,
            },
            {"field": "context.intent", "headerName": "Intent", "width": 300, "hide": False},
            {"field": "context.notes", "headerName": "Notes", "width": 500, "hide": False},
            {"field": "size", "headerName": "File size", "width": 80, "hide": False},
            {"field": "status", "headerName": "Status", "width": 80, "hide": False},
        ]

        self.columns: list[dict] = []

        # This is to be used to programatically add new columns, method still to be created.
        self.columns_with_default_col_def: list[dict] = [
            {
                "width": 100,
                "minWidth": 50,
                "maxWidth": None,  # None represents null in Python
                "hideable": True,
                "sortable": True,
                "resizable": True,
                "filterable": True,
                "groupable": True,
                "pinnable": True,
                "aggregable": True,
                "editable": False,
                "type": "string",
                "align": "left",
                "filterOperators": [
                    {"value": "contains"},
                    {"value": "equals"},
                    {"value": "startsWith"},
                    {"value": "endsWith"},
                    {"value": "isEmpty", "requiresFilterValue": False},
                    {"value": "isNotEmpty", "requiresFilterValue": False},
                    {"value": "isAnyOf"},
                ],
                "field": "id",
                "hide": True,
            }
        ]

        self.initial_state: dict = {"columns": {"columnVisibilityModel": {"id": False}}}

        for item in self.initial_column_config:
            self.columns.append(item)

        self.table_config: dict = {}
        self.table_config["columns"] = self.columns

        self.flattened_list_of_keys = []
        self.flattened_list_of_dataproducts_metadata: list[dict] = []
        self.rows: list[dict] = []

    def add_datagrid_row(self, row: dict) -> None:
        """Adds a dict of data to the datagrid row object.

        Args:
            row: A dictionary containing key-value pairs representing the data for the new row.
                The keys in the dictionary should correspond to the column names in the datagrid.
        """
        self.rows.append(row)

    def load_metadata_from_list(self, metadata_list: list[dict]) -> None:
        """
        Loads data from the metadata list into the in-memory store of the MUI DataGrid instance.

        This method clears the existing rows in the DataGrid and then iterates through the
        `self.metadata_list` adding each item as a new row.

        """
        self.rows.clear()
        for item in metadata_list:
            self.add_datagrid_row(item)

    def update_flattened_list_of_keys(self, metadata_file: dict) -> None:
        """
        Updates the `flattened_list_of_keys` attribute with new keys extracted from the specified
        metadata file.

        Args:
            metadata_file (dict): The path to the metadata file containing keys to be added.

        Raises:
            TypeError: If `metadata_file` is not a string.
        """
        for key in self.generate_metadata_keys_list(metadata_file, [], "", "."):
            if key not in self.flattened_list_of_keys:
                self.flattened_list_of_keys.append(key)

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

    def update_flattened_list_of_dataproducts_metadata(self, data_product_details):
        """
        Updates the internal list of data products with the provided metadata.

        This method adds the provided `data_product_details` dictionary to the internal
        `metadata_list` attribute. If the list is empty, it assigns an "id" of 1 to the
        first data product. Otherwise, it assigns an "id" based on the current length
        of the list + 1.

        Args:
            data_product_details: A dictionary containing the metadata for a data product.

        Returns:
            None
        """
        # Adds the first dictionary to the list
        if len(muiDataGridInstance.flattened_list_of_dataproducts_metadata) == 0:
            data_product_details["id"] = 1
        else:
            data_product_details["id"] = (
                len(muiDataGridInstance.flattened_list_of_dataproducts_metadata) + 1
            )

        muiDataGridInstance.flattened_list_of_dataproducts_metadata.append(data_product_details)


muiDataGridInstance = MuiDataGrid()
