"""Module to test InMemoryDataproductSearch"""
from ska_dataproduct_api.components.pv_interface.pv_interface import PVInterface
from ska_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_dataproduct_api.components.store.in_memory.in_memory import (
    InMemoryVolumeIndexMetadataStore,
)


def test_status():
    """Tests the status method."""
    # Call the method
    pv_interface = PVInterface()
    pv_interface.index_all_data_product_files_on_pv()
    metadata_store = InMemoryVolumeIndexMetadataStore()
    metadata_store.reload_all_data_products_in_index(pv_index=pv_interface.pv_index)
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)
    _ = mocked_search_store.filter_data(
        mui_data_grid_filter_model={},
        search_panel_options={
            "items": [
                {"field": "", "operator": "contains", "value": ""},
                {"field": "date_created", "operator": "greaterThan", "value": "2023-01-01"},
                {"field": "date_created", "operator": "lessThan", "value": ""},
            ],
            "logicOperator": "and",
        },
        users_user_group_list={},
    )

    response = mocked_search_store.status()

    # Assert expected response
    assert response == {
        "metadata_search_store_in_use": "In memory search store",
        "number_of_dataproducts": 15,
    }


def test_filter_data():
    """Method to test search of metadata"""
    pv_interface = PVInterface()
    pv_interface.index_all_data_product_files_on_pv()
    metadata_store = InMemoryVolumeIndexMetadataStore()
    metadata_store.reload_all_data_products_in_index(pv_index=pv_interface.pv_index)
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)
    expected_execution_block = "eb-m001-20230921-245"
    mui_data_grid_filter_model = {
        "items": [
            {
                "field": "execution_block",
                "operator": "contains",
                "id": 51411,
                "value": "m001-20230921",
                "fromInput": ":r4l:",
            }
        ],
        "logicOperator": "and",
        "quickFilterValues": [],
        "quickFilterLogicOperator": "and",
    }
    search_panel_options = {
        "items": [
            {"field": "date_created", "operator": "greaterThan", "value": ""},
            {"field": "date_created", "operator": "lessThan", "value": ""},
            {"field": "formFields", "keyPairs": [{"keyPair": "", "valuePair": ""}]},
        ],
        "logicOperator": "and",
    }
    filtered_list_of_data_product_metadata_files = mocked_search_store.filter_data(
        mui_data_grid_filter_model=mui_data_grid_filter_model,
        search_panel_options=search_panel_options,
        users_user_group_list=[],
    )

    assert (
        len(set(item["execution_block"] for item in filtered_list_of_data_product_metadata_files))
        == 1
    )
    assert (
        filtered_list_of_data_product_metadata_files[0]["execution_block"]
        == expected_execution_block
    )


def test_sort_list_of_dict_default():
    """Tests sorting by default key (`date_created`) in ascending order."""
    # Simulate some data with varying "date_created" values
    pv_interface = PVInterface()
    pv_interface.index_all_data_product_files_on_pv()
    metadata_store = InMemoryVolumeIndexMetadataStore()
    metadata_store.reload_all_data_products_in_index(pv_index=pv_interface.pv_index)
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)

    mocked_list_of_data = [
        {"name": "Product A", "date_created": "2024-08-20"},
        {"name": "Product B", "date_created": "2024-08-21"},
        {"name": "Product C", "date_created": "2024-08-19"},
    ]
    mocked_search_store.sort_list_of_dict(list_of_dict=mocked_list_of_data)
    # Assert the list is sorted by "date_created" in ascending order
    expected_order = [
        {"name": "Product B", "date_created": "2024-08-21"},
        {"name": "Product A", "date_created": "2024-08-20"},
        {"name": "Product C", "date_created": "2024-08-19"},
    ]
    assert mocked_list_of_data == expected_order
