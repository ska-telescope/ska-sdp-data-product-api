#!/usr/bin/env python
"""Basic test for the ska_sdp_dataproduct_api fastapi module."""


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/status")
    assert response.status_code == 200
    assert response.json()["api_running"] is True
    assert "api_version" in response.json()
    assert "startup_time" in response.json()
    assert "last_metadata_update_time" in response.json()
    assert "search_store_status" in response.json()
    assert "metadata_store_status" in response.json()


def test_reindex_data_products(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/reindexdataproducts")
    assert response.status_code == 202
    assert "Metadata is set to be re-indexed" in str(response.json())


def test_download_file(test_app):
    """Test if a file can be downloaded from the test files"""
    data = '{"execution_block": "eb-test-20200325-00001"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_download_folder(test_app):
    """Test if a folder can be downloaded from the test files"""
    data = '{"execution_block": "eb-m001-20191031-12345"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_data_product_metadata(test_app):
    """Test if metadata can be retrieved for a data product"""
    data = '{"execution_block": "eb-m005-20231031-12345"}'
    response = test_app.post("/dataproductmetadata", data=data)
    assert response.status_code == 200
    assert "Experimental run as part of XYZ-123" in str(response.json())


def test_in_memory_search(test_app):
    """This tests the in-memory precise search."""
    data = {
        "start_date": "2001-12-12",
        "end_date": "2032-12-12",
        "key_value_pairs": ["execution_block:eb-m001-20191031-12345"],
    }
    response = test_app.post("/dataproductsearch", json=data)
    assert response.status_code == 200
    assert response.json()[0]["execution_block"] == "eb-m001-20191031-12345"


def test_ingest_new_metadata(test_app):
    """Test if metadata for a new data product can be ingested via the
    REST API
    """
    execution_block_id = "eb-test-20191031-99999"

    data = {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "date_created": "2019-10-31",
        "execution_block": execution_block_id,
        "metadata_file": "",
        "context": {},
        "config": {},
        "files": [],
        "obscore": {},
    }

    response = test_app.post("/ingestnewmetadata", json=data)
    assert response.status_code == 200
    assert "New data product metadata received and search store index updated" in str(
        response.json()
    )


def test_in_memory_search_empty_key_value_list(test_app):
    """This tests the in-memory precise search."""
    data = {
        "start_date": "2001-12-12",
        "end_date": "2032-12-12",
        "key_value_pairs": [],
    }
    response = test_app.post("/dataproductsearch", json=data)
    assert response.status_code == 200
    assert len(response.json()) > 0


def test_in_memory_search_no_key_value_list(test_app):
    """This tests the in-memory precise search."""
    data = {"start_date": "2001-12-12", "end_date": "2032-12-12"}
    response = test_app.post("/dataproductsearch", json=data)
    assert response.status_code == 200
    assert len(response.json()) > 0


def test_in_faulty_data_search(test_app):
    """This tests the in-memory precise search."""
    data = {
        "start_date": "2001-12-13",
        "end_date": "2032-12-13",
        "key_value_pairs": ["execution_blockeb-m001-20191031-12345"],
    }
    response = test_app.post("/dataproductsearch", json=data)
    assert response.status_code == 400
