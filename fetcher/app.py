from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from starlette.responses import RedirectResponse

from common.database import init_cached_database
from fetcher.routers.omics import router as omics_router

app = FastAPI(title='Fetcher', default_response_class=ORJSONResponse)

app.include_router(router=omics_router)


@app.get('/', include_in_schema=False)
async def index():
    return RedirectResponse('/docs', status_code=302)
