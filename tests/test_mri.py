import os.path
from io import BytesIO
from pathlib import Path

import numpy as np
import pytest
import requests
from PIL import Image
from httpx import AsyncClient

from fetcher.app import app
from fastapi.testclient import TestClient

client = TestClient(app)


def test_get_mri_scans():
    r = client.get('mri_scans')
    r.raise_for_status()

    items = r.json()

    assert isinstance(items, list)
    assert all([isinstance(item, list) for item in items])

    assert all([all(isinstance(item_, dict) for item_ in item) for item in items])

    r = requests.head(r.json()[0][0]['files'][0])

    r.raise_for_status()


def test_get_segmentation_slice_overlayed():
    r = client.get("/segmentation_slice", params=dict(patient='TCGA-AO-A03M'))
    # r = client.get('segmentation_slice', params=dict(patient='TCGA-AO-A03M'))
    r.raise_for_status()
    img = Image.open(BytesIO(r.content))

    gt = Image.open(Path(Path(__file__).parent.parent.as_posix(), 'resources/mri/overlayed.png'))

    np.testing.assert_allclose(np.array(gt), np.array(img))
