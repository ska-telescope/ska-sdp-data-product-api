"""Module to test InMemoryDataproductSearch"""
import json

from ska_sdp_dataproduct_api.components.search.in_memory.in_memory_search import (
    InMemoryDataproductSearch,
)
from ska_sdp_dataproduct_api.components.store.store_factory import select_metadata_store_class


def test_status():
    """Tests the status method."""

    metadata_store = select_metadata_store_class()

    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)

    # Call the method
    response = mocked_search_store.status()
    # Assert expected response
    assert response == {
        "metadata_search_store_in_use": "In memory search store",
        "number_of_dataproducts": 13,
    }


def test_search_metadata_execution_block():
    """Tests the search_metadata method, ensuring only one execution block entry."""

    metadata_store = select_metadata_store_class()
    mocked_search_store = InMemoryDataproductSearch(metadata_store=metadata_store)

    # Call the method with expected data
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
