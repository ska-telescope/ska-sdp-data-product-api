#!/usr/bin/env python
"""Basic test for the ska_sdp_qa_data_api fastapi module."""


def test_ping_main(test_app):
    """Can we hit the ping endpoint"""
    response = test_app.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_filelist(test_app):
    """Test to see if a file list can be retrieved"""
    response = test_app.get("/filelist")
    assert response.status_code == 200
    assert response.json() == {
        "filelist": [{"id": 0, "filename": "testfile.txt"}]
    }
