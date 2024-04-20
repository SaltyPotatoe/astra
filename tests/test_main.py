import os
import sys

from fastapi.testclient import TestClient

from astra.main import app  # noqa: E402

with TestClient(app) as client:

    def test_read_heartbeat():
        response = client.get("/api/heartbeat/Callisto")
        assert response.status_code == 200
        assert response.json()["data"]["error_free"] is True
