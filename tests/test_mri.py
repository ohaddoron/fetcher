import requests
from fetcher.app import app
from fastapi.testclient import TestClient


def test_get_mri_scans():
    with TestClient(app) as client:
        r = client.get('mri_scans')
        r.raise_for_status()

        items = r.json()

        assert isinstance(items, list)
        assert all([isinstance(item, list) for item in items])

        assert all([all(isinstance(item_, dict) for item_ in item) for item in items])

        r = requests.head(r.json()[0][0]['files'][0])

        r.raise_for_status()
