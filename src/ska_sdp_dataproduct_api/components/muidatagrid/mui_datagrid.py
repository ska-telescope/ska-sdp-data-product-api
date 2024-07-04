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

        self.rows: list[dict] = []

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


muiDataGridInstance = MuiDataGrid()
