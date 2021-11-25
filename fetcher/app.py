from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from starlette.requests import Request
from starlette.responses import RedirectResponse

from fetcher.routers.omics import router as omics_router
from fetcher.routers.mri import router as mri_router

app = FastAPI(title='Fetcher', default_response_class=ORJSONResponse)

app.include_router(router=omics_router)
app.include_router(router=mri_router)


@app.get('/', include_in_schema=False)
async def index(request: Request):
    return RedirectResponse(request.scope.get("root_path") + '/docs', status_code=302)
