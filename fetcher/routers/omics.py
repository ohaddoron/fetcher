from functools import lru_cache

from fastapi import APIRouter, Query, Depends
from starlette.background import BackgroundTasks
from starlette.responses import StreamingResponse
import typing as tp
import orjson

from common.database import init_database
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
    # return [dict(patient=data['patient'], data=data) for data in cursor]
    async for item in cursor:
        yield orjson.dumps(item).decode() + ',\n'


@router.get('/survival')
async def get_survival(background_task: BackgroundTasks, patients: tp.Tuple[str] = Query(None)):
    return StreamingResponse(aggregate_db('Survival', patients), background=background_task,
                             media_type='application/json')


@router.get('/copy_number')
async def get_survival(background_task: BackgroundTasks, patients: tp.Tuple[str] = Query(None)):
    return StreamingResponse(aggregate_db('CopyNumber', patients), background=background_task,
                             media_type='application/json')


@router.get('/clinical_data')
async def get_clinical_data(background_task: BackgroundTasks, patients: tp.Tuple[str] = Query(None)):
    return StreamingResponse(aggregate_db('ClinicalData', patients), background=background_task,
                             media_type='application/json')


@lru_cache
def _get_mutations():
    settings = get_settings()
    db = init_database(config_name=settings.db_name)
    return db['SomaticMutation'].distinct('name')


@router.get('/patients_by_mutation')
async def get_patients_by_mutation(mutation: str = Query(None, enum=_get_mutations()),
                                   settings: Settings = Depends(get_settings)
                                   ):
    db = init_database(config_name=settings.db_name)
    return db['SomaticMutation'].find({'name': mutation, 'value': 1}).distinct('patient')


@router.get('/patients_age')
async def get_patients_age(patients: tp.List[str] = Query(None),
                           settings: Settings = Depends(get_settings)):
    db = init_database(config_name=settings.db_name, async_flag=True)
    cursor = db['ClinicalData'].find({'patients': {"$in": patients}})

    out = []
    async for doc in cursor:
        out.append(dict(patient=doc['patient'], age=doc['age_at_diagnosis'] / 365))
    return out
