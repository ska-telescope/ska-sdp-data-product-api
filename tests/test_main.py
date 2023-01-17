#!/usr/bin/env python
"""Basic test for the ska_sdp_qa_data_api fastapi module."""
import pytest
from fastapi import HTTPException

from ska_sdp_data_product_api.main import TreeIndex, getfilenames


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "The application is running"}


def test_dataproductlist(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("dataproductlist")
    assert response.status_code == 200
    assert str(response.json()).__contains__(
        "/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/ska-data-product.yaml"
    )


def test_getfilenames_unhappy_path():
    """This tests the expected error of 404 if a non existing path is passed
    to the getfilenames function"""

    data_product_index = TreeIndex(root_tree_item_id="root", tree_data={})
    metadata_file = ""
    with pytest.raises(HTTPException) as exc_info:
        _ = getfilenames(
            "Non_existing_path", data_product_index, metadata_file
        )
    assert isinstance(exc_info.value, HTTPException)
    assert exc_info.value.status_code == 404


def test_download_file(test_app):
    """Test if a file can be downloaded from the test files"""
    data = '{"fileName": "TestDataFile1.txt","relativeFileName": \
        "product/eb_id_1/ska-sub-system/scan_id_1/pb_id_1/TestDataFile1.txt"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_download_folder(test_app):
    """Test if a folder can be downloaded from the test files"""
    data = '{"fileName": "eb_id_1","relativeFileName": \
        "product/eb_id_1/"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_dataproductmetadata(test_app):
    """Test if metadata can be retrieved for a data product"""
    data = '{"fileName": "ska-data-product.yaml","relativeFileName": \
    "product/eb_id_2/ska-sub-system/scan_id_2/pb_id_2/ska-data-product.yaml"}'
    response = test_app.post("/dataproductmetadata", data=data)
    assert response.status_code == 200
    assert str(response.json()).__contains__(
        "Experimental run as part of XYZ-123"
    )
