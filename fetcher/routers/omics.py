from fastapi import APIRouter, Query
from starlette.background import BackgroundTasks
from starlette.responses import StreamingResponse
import typing as tp
import orjson

from common.database import init_database

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
