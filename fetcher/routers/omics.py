import os
from functools import lru_cache
from pathlib import Path

from bson import json_util
from fastapi import APIRouter, Query, Depends, Body
from loguru import logger
from starlette.background import BackgroundTasks
from starlette.responses import StreamingResponse
import typing as tp
import orjson

from common.database import init_database as init_database
from fetcher.config import get_settings, Settings

router = APIRouter(tags=['omics'])


async def aggregate_db(collection, patients):
    ppln = [
        {
            "$match": {
                'case_submitter_id':
                    {"$in":
                         patients
                     }
            }
        } if patients else
        {"$match":
             {}
         },
        {
            "$group": {
                "_id": "$sample",
                "patient": {
                    "$first": "$patient"
                },
                "names": {
                    "$push": "$name"
                },
                "values": {
                    "$push": "$value"
                }
            }
        },
        {
            "$project": {
                "field": {
                    "$map": {
                        "input": {
                            "$zip": {
                                "inputs": [
                                    "$names",
                                    "$values"
                                ]
                            }
                        },
                        "as": "el",
                        "in": {
                            "name": {
                                "$arrayElemAt": [
                                    "$$el",
                                    0
                                ]
                            },
                            "value": {
                                "$arrayElemAt": [
                                    "$$el",
                                    1
                                ]
                            }
                        }
                    }
                },
                "patient": 1,
                "sample": "$_id",
                "_id": 0
            }
        }
    ]
    db = init_database(config_name='omics-database', async_flag=True)
    cursor = db[collection].aggregate(ppln, allowDiskUse=True)
    return await cursor.to_list(None)


@router.get('/survival')
async def get_survival(patients: tp.Tuple[str] = Query(None)):
    return await aggregate_db(collection='Survival', patients=patients)


@router.get('/copy_number')
async def get_survival(patients: tp.Tuple[str] = Query(None)):
    return await aggregate_db('CopyNumber', patients)


@router.get('/clinical_data')
async def get_clinical_data(patients: tp.Tuple[str] = Query(None)):
    return await aggregate_db('ClinicalData', patients)


@lru_cache
def _get_mutations():
    settings = get_settings()
    db = init_database(config_name=settings.db_name)
    return db['SomaticMutation'].distinct('name')


@router.get('/patients_by_mutation')
async def get_patients_by_mutation(mutation: str = Query(None, enum=_get_mutations()),
                                   mutation_status: bool = True,
                                   settings: Settings = Depends(get_settings)
                                   ):
    db = init_database(config_name=settings.db_name)
    return db['SomaticMutation'].find({'name': mutation, 'value': int(mutation_status)}).distinct('patient')


@router.post('/patients_age')
async def get_patients_age(patients: tp.List[str] = Body(None),
                           settings: Settings = Depends(get_settings)):
    db = init_database(config_name=settings.db_name, async_flag=True)
    ppln = [
        {
            '$match': {
                'patient': {
                    '$in': patients
                },
                'name': 'age_at_diagnosis'
            }
        }, {
            '$group': {
                '_id': {
                    'patient': '$patient',
                    'age': '$value'
                }
            }
        }, {
            '$project': {
                'patient': '$_id.patient',
                'age': '$_id.age',
                'type': {
                    '$type': '$_id.age'
                },
                '_id': 0
            }
        }, {
            '$match': {
                'type': 'double'
            }
        }, {
            '$project': {
                'type': 0
            }
        }, {
            '$addFields': {
                'age': {
                    '$divide': [
                        '$age', 365
                    ]
                }
            }
        }
    ]
    print(json_util.dumps(ppln, indent=2))
    cursor = db['ClinicalData'].aggregate(ppln)

    return await cursor.to_list(None)


def _get_column_names(config_name: str = 'omics-database'):
    db = init_database(config_name)
    return db.list_collection_names()


@router.get('/column_names')
async def get_column_names(settings: Settings = Depends(get_settings)):
    return _get_column_names(settings.db_name)


@router.get('/feature_names')
async def get_feature_names(col: str = Query(None, enum=_get_column_names()),
                            settings: Settings = Depends(get_settings)):
    db = init_database(settings.db_name)
    return db[col].distinct('name')
