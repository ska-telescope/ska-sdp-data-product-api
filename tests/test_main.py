#!/usr/bin/env python
"""Basic test for the ska_sdp_qa_data_api fastapi module."""


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/status")
    assert response.status_code == 200
    assert response.json() == {"API_running": True, "Search_enabled": False}


def test_dataproductlist(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/dataproductlist")
    assert response.status_code == 200
    assert str(response.json()).__contains__(
        "product/eb-m001-20221212-12345/ska-data-product.yaml"
    )


def test_download_file(test_app):
    """Test if a file can be downloaded from the test files"""
    data = (
        '{"fileName": "TestDataFile1.txt","relativeFileName": \
        "product/eb-m001-20221212-12345/ska-sub-system/scan_id_1/pb_id_1'
        + '/TestDataFile1.txt"}'
    )
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_download_folder(test_app):
    """Test if a folder can be downloaded from the test files"""
    data = '{"fileName": "eb-m001-20221212-12345","relativeFileName": \
        "product/eb-m001-20221212-12345"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_dataproductmetadata(test_app):
    """Test if metadata can be retrieved for a data product"""
    data = '{"fileName": "ska-data-product.yaml","relativeFileName": \
    "product/eb-m001-20221212-12345/ska-data-product.yaml"}'
    response = test_app.post("/dataproductmetadata", data=data)
    assert response.status_code == 200
    assert str(response.json()).__contains__(
        "Experimental run as part of XYZ-123"
    )


def test_dataproductsearch_unhappy_path(test_app):
    """Test the unhappy data product search for when the ES instance is
    not available, should return a 503 service not available error"""
    data = {
        "start_date": "2001-12-12",
        "end_date": "2032-12-12",
        "key_pair": "execution_block:eb-m001-20191031-12345",
    }
    response = test_app.post("/dataproductsearch", json=data)
    assert response.status_code == 503
