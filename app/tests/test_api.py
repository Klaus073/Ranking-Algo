import os
from fastapi.testclient import TestClient

os.environ["ENVIRONMENT"] = "test"

from app.app import app  # noqa: E402


client = TestClient(app)


def test_config_endpoint():
    r = client.get("/api/config")
    assert r.status_code == 200
    assert "config_version" in r.json()




