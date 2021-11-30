import asyncio
import os.path
import pickle
import tempfile
from http.client import HTTPException
from io import BytesIO
from pathlib import Path
from aiocache import cached
from fastapi import APIRouter, Query, Depends
import httpx
import aiofiles
import numpy as np
import typing as tp
import cv2
import skimage
from motor.motor_asyncio import AsyncIOMotorDatabase

from starlette.responses import StreamingResponse
import skimage.exposure
from common.database import init_database as init_database
from common.utils import read_dicom_images
from common.les_files import read_all_maps_from_les_file
from PIL import Image
from matplotlib import pyplot as plt

router = APIRouter(tags=['mri'])


async def _download_dcm_file(link: str, index: int, directory: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(link)
        async with aiofiles.open(Path(directory, f'{index}.dcm'), 'wb') as f:
            await f.write(r.content)


@cached(ttl=300)
async def _download_segmentation_file(link: str, output_size: tp.Tuple[int, int], num_stacks: int):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(link)
        with tempfile.TemporaryDirectory() as t:
            async with aiofiles.open(Path(t, f'{link.split("/")[-1]}.les'), 'wb') as f:
                await f.write(r.content)
            seg_file = read_all_maps_from_les_file(Path(t, f'{link.split("/")[-1]}.les').as_posix())[0]
            mask = np.zeros((num_stacks,) + output_size, dtype=np.uint8)

            header = seg_file['header']
            mask[header[2][0]: header[2][1] + 1, header[0][0]: header[0][1] + 1, header[1][0]: header[1][1] + 1] = \
                seg_file['data'] * 255

    return mask


async def _get_segmentation_mask(link, output_size: tp.Tuple[int, int], num_stacks: int, slice: int):
    mask = await _download_segmentation_file(link=link, output_size=output_size, num_stacks=num_stacks)
    return mask[slice]


async def _fetch_image_stack(patient: str, sample_number: int, segmented_files_only: bool = False):
    patient_samples = (await get_mri_scans(patients=[patient], segmented_files_only=segmented_files_only))[0]
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


def _image_to_rgb(image: np.ndarray) -> np.ndarray:
    image = image[..., np.newaxis]
    return np.repeat(image, repeats=3, axis=-1)


def _get_mri_patients():
    db = init_database(config_name='omics-database', async_flag=False)
    return db['MRIScans'].find().distinct("patient")


def _get_mri_patients_with_segmentation_file():
    db = init_database(config_name='omics-database', async_flag=False)
    return db['MRIScans'].find({'segmentation_file': {"$exists": True}}).distinct("patient")


@router.get('/mri_scans', description='Draws the mri scans for the patients '
                                      'from the database. Returns a list of '
                                      'links for each patient. Links are '
                                      'sorted according to slice location')
async def get_mri_scans(patients: tp.Union[tp.List[str]] = Query(None), segmented_files_only: bool = False):
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
    if segmented_files_only:
        ppln.insert(0, {'$match': {"segmentation_file": {"$exists": True}}})
    if patients:
        ppln.insert(0, {'$match': {"patient": {"$in": patients}}})
    db = init_database(async_flag=True, config_name='omics-database')
    cursor = db['MRIScans'].aggregate(ppln)
    results = []
    async for result in cursor:
        for sample in result['samples']:
            sample['files'] = sorted(sample['files'])
        results.append(result)
    return [result['samples'] for result in results]


@router.get('/mri_scan_stack', description='Stacks together a single MRI stack for a single patient')
async def get_mri_scan_stack(
        patient: str = Query(default=None, enum=_get_mri_patients()),
        sample_number: int = 0,
):
    if patient is None:
        raise HTTPException(400, 'patient name cannot be None')
    dcm_image = await _fetch_image_stack(patient=patient, sample_number=sample_number,
                                         )
    return StreamingResponse(BytesIO(pickle.dumps(dcm_image)))


@router.get('/mri_scan_slice')
async def get_mri_scan_slice(
        patient: str = Query(None, enum=_get_mri_patients()),
        sample_number: int = Query(default=0, ge=0),
        slice_number: int = Query(default=0, ge=0)):
    dcm_image: np.ndarray = (await _fetch_image_stack(patient=patient, sample_number=sample_number))[slice_number]
    img = _stretch_image(dcm_image)
    _, im_png = cv2.imencode('.png', img)
    return StreamingResponse(BytesIO(im_png.tobytes()), media_type="image/png")


@router.get('/segmentation_slice')
async def get_segmentation_slice_overlayed(
        patient: str = Query(None, enum=_get_mri_patients_with_segmentation_file()),
        sample_number: int = Query(default=0, ge=0),
        slice_number: int = Query(default=0, ge=0),
        alpha: float = Query(default=0.6, ge=0., le=1.)
):
    dcm_image: np.ndarray = \
        (await _fetch_image_stack(patient=patient, sample_number=sample_number, segmented_files_only=True)
         )[slice_number]
    img = _stretch_image(dcm_image)
    img = _image_to_rgb(img)

    db = init_database(config_name='omics-database')
    items = list(db['MRIScans'].find({'patient': patient, 'segmentation_file': {'$exists': True}}))
    mask = await _get_segmentation_mask(items[0]['segmentation_file'], output_size=img.shape[:2],
                                        num_stacks=len(items[0]['value']), slice=slice_number
                                        )
    image_2 = img.copy()

    image_2[..., 1:2] = np.maximum(image_2[..., 1:2], mask[..., np.newaxis])

    result = (1 - alpha) * img + alpha * image_2
    _, im_png = cv2.imencode('.png', result)
    return StreamingResponse(BytesIO(im_png.tobytes()), media_type="image/png")
