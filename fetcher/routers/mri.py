import asyncio
import pickle
import tempfile
from http.client import HTTPException
from io import BytesIO
from pathlib import Path

from fastapi import APIRouter, Query
import httpx
import aiofiles
import numpy as np
import typing as tp
import cv2
import skimage
from starlette.responses import StreamingResponse
import skimage.exposure
from common.database import init_database
from common.utils import read_dicom_images

router = APIRouter(tags=['mri'])


async def _download_dcm_file(link: str, index: int, directory: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(link)
        async with aiofiles.open(Path(directory, f'{index}.dcm'), 'wb') as f:
            await f.write(r.content)


async def _fetch_image_stack(patient: str, sample_number: int):
    patient_samples = (await get_mri_scans(patients=[patient]))[0]
    try:
        patient_sample = patient_samples[sample_number]
    except IndexError:
        raise HTTPException(f'Sample number is out of bounds for {patient}')
    stacks_links = patient_sample['files']
    with tempfile.TemporaryDirectory() as t:
        tasks = [_download_dcm_file(link=link, index=index, directory=t) for index, link in enumerate(stacks_links)]

        await asyncio.gather(*tasks)

        dcm_image = read_dicom_images(t)
    return dcm_image


def _stretch_image(image: np.ndarray) -> np.ndarray:
    image = (255 * (image / np.max(image))).astype(np.uint8)

    image = (skimage.exposure.equalize_adapthist(
        image, kernel_size=15) * 255).astype(np.uint8)
    return image


def _get_mri_patients():
    db = init_database(config_name='omics-database', async_flag=False)
    return db['MRIScans'].find().distinct("patient")


@router.get('/mri_scans', description='Draws the mri scans for the patients '
                                      'from the database. Returns a list of '
                                      'links for each patient. Links are '
                                      'sorted according to slice location')
async def get_mri_scans(patients: tp.Union[tp.List[str]] = Query(None)
                        ):
    ppln = [
        {
            '$project': {
                'patient': 1,
                'sample': 1,
                'value': 1
            }
        }, {
            '$addFields': {
                'temp': {
                    'sample': '$sample',
                    'files': '$value',
                    'patient': '$patient',
                    'series_uid': "$series_uid"
                }
            }
        }, {
            '$group': {
                '_id': '$patient',
                'samples': {
                    '$push': '$temp'
                }
            }
        }, {
            '$project': {
                'patient': '$_id',
                '_id': 0,
                'samples': 1
            }
        }
    ]
    if patients:
        ppln.insert(0, {'$match': {"patient": {"$in": patients}}})
    db = init_database(async_flag=True, config_name='omics-database')
    results = await db['MRIScans'].aggregate(ppln).to_list(None)
    for result in results:
        for sample in result['samples']:
            sample['files'] = sorted(sample['files'])
    return [result['samples'] for result in results]


@router.get('/mri_scan_stack', description='Stacks together a single MRI stack for a single patient')
async def get_mri_scan_stack(
        patient: str = Query(default=None, enum=_get_mri_patients()),
        sample_number: int = 0):
    dcm_image = await _fetch_image_stack(patient=patient, sample_number=sample_number)
    return StreamingResponse(BytesIO(pickle.dumps(dcm_image)))


@router.get('/mri_scan_slice')
async def get_mri_scan_slice(
        patient: str = Query(None, enum=_get_mri_patients()),
        sample_number: int = 0,
        slice_number: int = 0):
    dcm_image: np.ndarray = (await _fetch_image_stack(patient=patient, sample_number=sample_number))[slice_number]
    img = _stretch_image(dcm_image)
    _, im_png = cv2.imencode('.png', img)
    return StreamingResponse(BytesIO(im_png.tobytes()), media_type="image/png")
