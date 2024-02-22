#!/usr/bin/env python
"""Basic test for the ska_sdp_dataproduct_api fastapi module."""


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/status")
    assert response.status_code == 200
    assert response.json()["API_running"] is True
    assert "Search_enabled" in response.json()
    assert "Date_modified" in response.json()
    assert "Version" in response.json()
    assert "Indexing" in response.json()


def test_reindex_data_products(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/reindexdataproducts")
    assert response.status_code == 202
    assert "Metadata is set to be cleared and re-indexed" in str(response.json())


def test_data_product_list(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/dataproductlist")
    assert response.status_code == 200
    assert "eb-m001-20221212-12345/ska-data-product.yaml" in str(response.json())
    assert "pb-notebookpo-20240201-54576/ska-data-product.yaml" in str(response.json())
    assert "pb-notebookvr-20240201-54576/ska-data-product.yaml" in str(response.json())
    # make sure that the response JSON contains 7 data products,
    # and therefore that the 3 YAML files missing execution_block attributes
    # have not been ingested
    assert len(response.json()) == 11


def test_download_file(test_app):
    """Test if a file can be downloaded from the test files"""
    data = (
        '{"fileName": "TestDataFile1.txt","relativePathName": \
        "eb-m001-20221212-12345/ska-sub-system/scan_id_1/pb_id_1'
        + '/TestDataFile1.txt"}'
    )
    response = test_app.post("/download", data=data)
    assert response.status_code == 200

    data = '{"fileName": "pb-notebookvr-20240201-54576","relativePathName": \
        "eb-notebook-20240201-54576/ska-sdp/pb-notebookvr-20240201-54576"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_download_folder(test_app):
    """Test if a folder can be downloaded from the test files"""
    data = '{"fileName": "eb-m001-20221212-12345","relativePathName": \
        "eb-m001-20221212-12345"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_data_product_metadata(test_app):
    """Test if metadata can be retrieved for a data product"""
    data = '{"fileName": "ska-data-product.yaml","relativePathName": \
    "eb-m001-20221212-12345/ska-data-product.yaml"}'
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
    data = {
        "start_date": "2001-12-12",
        "end_date": "2032-12-12",
    }
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
