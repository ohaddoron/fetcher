import asyncio

import pytest
from starlette.testclient import TestClient
from fetcher.app import app
from fetcher.routers.omics import aggregate_db


def test_get_feature_names():
    with TestClient(app) as client:
        r = client.get('/feature_names', params=dict(col='ClinicalData'))
        r.raise_for_status()

        assert isinstance(r.json(), list)
        assert r.json()


@pytest.mark.parametrize('patient', [None, ('TCGA-E2-A14U',)])
def test_get_clinical_data(patient):
    with TestClient(app) as client:
        r = client.get('/clinical_data', params=dict(patients=patient))
        r.raise_for_status()

    assert r.json()
