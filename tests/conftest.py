"""Helper class for tests."""

import pytest
from fastapi.testclient import TestClient

from ska_dataproduct_api.api.main import app


@pytest.fixture(scope="module")
def test_app():
    """Helper fixture for the client."""
    client = TestClient(app)
    yield client
