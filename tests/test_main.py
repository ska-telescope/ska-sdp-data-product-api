#!/usr/bin/env python
"""Basic test for the ska_sdp_qa_data_api fastapi module."""
from fastapi.testclient import TestClient

from ska_sdp_data_product_api.main import app

client = TestClient(app)


def test_ping_root():
    """Ping test"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}


def test_ping_filelist():
    """Ping test"""
    response = client.get("/filelist")
    assert response.status_code == 200
