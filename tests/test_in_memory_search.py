"""Module to test InMemoryDataproductSearch"""
import json

from ska_sdp_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_sdp_dataproduct_api.components.store.store_factory import select_metadata_store_class


def test_status():
    """Tests the status method."""
    # Call the method
    metadata_store = select_metadata_store_class()
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)
    response = mocked_search_store.status()

    # Assert expected response
    assert response == {
        "metadata_search_store_in_use": "In memory search store",
        "number_of_dataproducts": 13,
    }


def test_search_metadata_execution_block_with_valid_date_and_no_eb():
    """Tests the search_metadata method, ensuring only one execution block entry."""
    # Call the method with expected data
    metadata_store = select_metadata_store_class()
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)
    expected_execution_block = "eb-notebook-20240201-54576"

    # Call the method
    response = mocked_search_store.search_metadata(
        start_date="2024-02-01", end_date="2024-02-02", metadata_key_value_pairs=None
    )

    # Assert expected response
    response_data = json.loads(response)

    # Check if there's only one unique execution block
    assert len(set(item["execution_block"] for item in response_data)) == 1
    assert response_data[0]["execution_block"] == expected_execution_block


def test_search_metadata_execution_block_with_valid_date_and_eb():
    """Tests the search_metadata method, ensuring only one execution block entry."""
    # Call the method with expected data
    metadata_store = select_metadata_store_class()
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)
    expected_execution_block = "eb-notebook-20240201-54576"
    metadata_key_value_pairs = [
        {"metadata_key": "execution_block", "metadata_value": "eb-notebook-20240201-54576"}
    ]

    # Call the method
    response = mocked_search_store.search_metadata(
        start_date="2020-01-01",
        end_date="2030-12-31",
        metadata_key_value_pairs=metadata_key_value_pairs,
    )

    # Assert expected response
    response_data = json.loads(response)

    # Check if there's only one unique execution block
    assert len(set(item["execution_block"] for item in response_data)) == 1
    assert response_data[0]["execution_block"] == expected_execution_block


def test_search_metadata():
    """Method to test search of metadata"""
    metadata_store = select_metadata_store_class()
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
    metadata_list = mocked_search_store.filter_data(
        mui_data_grid_filter_model=mui_data_grid_filter_model,
        search_panel_options=search_panel_options,
        users_user_group_list=[],
    )

    assert len(set(item["execution_block"] for item in metadata_list)) == 1
    assert metadata_list[0]["execution_block"] == expected_execution_block
