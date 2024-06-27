"""This module contains the class object used with the MUI DataGrid component in front end
applications"""


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
                "headerName": "execution_block",
                "width": 250,
                "hide": False,
            },
            {"field": "date_created", "headerName": "date_created", "width": 150, "hide": False},
            {"field": "context.observer", "headerName": "observer", "width": 150, "hide": False},
            {
                "field": "config.processing_block",
                "headerName": "processing_block",
                "width": 250,
                "hide": False,
            },
            {"field": "context.intent", "headerName": "Intent", "width": 300, "hide": False},
            {"field": "context.notes", "headerName": "notes", "width": 500, "hide": False},
            {"field": "size", "headerName": "file_size", "width": 80, "hide": False},
            {"field": "status", "headerName": "status", "width": 80, "hide": False},
        ]

        self.columns: list[dict] = [
            {"field": "id", "hide": True},
        ]

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
            existing_options = self.get_column_keys(item)
            self.columns.append(existing_options)
            column_with_default_col_def = self.get_default_col_def(**item)
            self.columns_with_default_col_def.append(column_with_default_col_def)

        self.table_config: dict = {}
        self.table_config["columns"] = self.columns
        self.table_config["columnsWithDefaultColDef"] = self.columns_with_default_col_def
        self.table_config["initialState"] = self.initial_state

        self.rows: list[dict] = []

    def get_default_col_def(self, **kwargs) -> dict:
        """
        Returns a column definition dictionary with defaults filled and
        specified values merged.

        Args:
            **kwargs: Keyword arguments representing column properties to override defaults.

        Returns:
            A dictionary containing all column properties.
        """

        # Start with a copy of the default column
        column = self.default_column.copy()

        # Update the column with specified keyword arguments
        column.update(kwargs)

        return column

    def get_column_keys(self, column_options: dict) -> dict:
        """
        Returns a dictionary containing all keys from column_options that exist in the
        default_columns.

        Args:
            column_options (dict): A dictionary containing potential column options.

        Returns:
            dict: A dictionary containing only the keys from column_options that are present in
            the default_columns.
        """
        existing_keys = {
            key: value for key, value in column_options.items() if key in self.default_column
        }
        return existing_keys

    def add_datagrid_row(self, row: dict) -> None:
        """Adds a dict of data to the datagrid row object.

        Args:
            row: A dictionary containing key-value pairs representing the data for the new row.
                The keys in the dictionary should correspond to the column names in the datagrid.
        """
        self.rows.append(row)

    def load_inmemory_store_data(self, inmemorystore) -> None:
        """
        Loads data from the metadata list into the in-memory store of the MUI DataGrid instance.

        This method clears the existing rows in the DataGrid and then iterates through the
        `self.metadata_list` adding each item as a new row.

        """
        self.rows.clear()
        for item in inmemorystore.metadata_list:
            self.add_datagrid_row(item)


muiDataGridinstance = MuiDataGrid()
