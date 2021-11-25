from starlette.testclient import TestClient
from fetcher.app import app

def test_get_feature_names():
    with TestClient(app) as client:
        r = client.get('/feature_names', params=dict(col='ClinicalData'))
        r.raise_for_status()

        assert isinstance(r.json(), list)
        assert r.json()
