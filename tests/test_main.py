#!/usr/bin/env python
"""Basic test for the ska_sdp_dataproduct_api fastapi module."""
import os
import shutil


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/status")
    assert response.status_code == 200
    assert response.json() == {"API_running": True, "Search_enabled": False}


def test_reindexdataproducts(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/reindexdataproducts")
    assert response.status_code == 200
    assert "Metadata store cleared and re-indexed" in str(response.json())


def test_dataproductlist(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/dataproductlist")
    assert response.status_code == 200
    assert "product/eb-m001-20221212-12345/ska-data-product.yaml" in str(
        response.json()
    )

    # make sure that the response JSON contains 7 data products,
    # and therefore that the 3 YAML files missing execution_block attributes
    # have not been ingested
    assert len(response.json()) == 7


def test_download_file(test_app):
    """Test if a file can be downloaded from the test files"""
    data = (
        '{"fileName": "TestDataFile1.txt","relativePathName": \
        "product/eb-m001-20221212-12345/ska-sub-system/scan_id_1/pb_id_1'
        + '/TestDataFile1.txt"}'
    )
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_download_folder(test_app):
    """Test if a folder can be downloaded from the test files"""
    data = '{"fileName": "eb-m001-20221212-12345","relativePathName": \
        "product/eb-m001-20221212-12345"}'
    response = test_app.post("/download", data=data)
    assert response.status_code == 200


def test_dataproductmetadata(test_app):
    """Test if metadata can be retrieved for a data product"""
    data = '{"fileName": "ska-data-product.yaml","relativePathName": \
    "product/eb-m001-20221212-12345/ska-data-product.yaml"}'
    response = test_app.post("/dataproductmetadata", data=data)
    assert response.status_code == 200
    assert "Experimental run as part of XYZ-123" in str(response.json())


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


def test_ingestjson(test_app):
    """Test if metadata for a new dataproduct can be ingested via the
    REST API
    """
    execution_block_id = "eb-rest-00000000-99999"

    data = {
        "interface": "http://schema.skao.int/ska-data-product-meta/0.1",
        "execution_block": execution_block_id,
        "context": {"observer": "REST ingest", "intent": "", "notes": ""},
        "config": {
            "processing_block": "",
            "processing_script": "",
            "image": "",
            "version": "",
            "commit": "",
            "cmdline": "",
        },
        "files": [],
        "obscore": {
            "access_estsize": 0,
            "access_format": "application/unknown",
            "access_url": "0",
            "calib_level": 0,
            "dataproduct_type": "MS",
            "facility_name": "SKA",
            "instrument_name": "SKA-LOW",
            "o_ucd": "stat.fourier",
            "obs_collection": "Unknown",
            "obs_id": "",
            "obs_publisher_did": "",
            "pol_states": "XX/XY/YX/YY",
            "pol_xel": 0,
            "s_dec": 0,
            "s_ra": 0.0,
            "t_exptime": 5.0,
            "t_max": 57196.962848574476,
            "t_min": 57196.96279070411,
            "t_resolution": 0.9,
            "target_name": "",
        },
    }

    response = test_app.post("/ingestjson", json=data)
    assert response.status_code == 200
    assert response.json()["execution_block"] == execution_block_id

    # clean up after test by deleting the data product metadata file
    # and the directory containing it
    path = os.path.dirname(response.json()["metadata_file"])
    if os.path.exists(path):
        shutil.rmtree(path)
