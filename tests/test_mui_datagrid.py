"""Module to test mui_datagrid"""
from ska_sdp_dataproduct_api.components.muidatagrid.mui_datagrid import muiDataGridInstance


def test_sort_list_of_dict_default():
    """Tests sorting by default key (`date_created`) in ascending order."""
    # Simulate some data with varying "date_created" values
    mocked_list_of_data = [
        {"name": "Product A", "date_created": "2024-08-20"},
        {"name": "Product B", "date_created": "2024-08-21"},
        {"name": "Product C", "date_created": "2024-08-19"},
    ]
    muiDataGridInstance.sort_list_of_dict(list_of_dict=mocked_list_of_data)
    # Assert the list is sorted by "date_created" in ascending order
    expected_order = [
        {"name": "Product B", "date_created": "2024-08-21"},
        {"name": "Product A", "date_created": "2024-08-20"},
        {"name": "Product C", "date_created": "2024-08-19"},
    ]
    assert mocked_list_of_data == expected_order
