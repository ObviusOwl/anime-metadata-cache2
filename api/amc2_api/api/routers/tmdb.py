from fastapi import APIRouter, Depends, HTTPException

from amc2_api.persistence import ObjectNotFound

from ..dependencies import Databases
from ..responses import PersistedResponse, PersistedStatResponse
from ..config import settings


class TmdbJsonResponse(PersistedResponse):
    media_type = 'text/json'
    charset = "utf-8"


router = APIRouter(
    prefix="/tmdb",
    tags=["tmdb"],
)


@router.get("/shows/{lang}/{sid}", response_class=TmdbJsonResponse)
def get_show(lang:str, sid: str, dbs: Databases = Depends()):
    try:
        obj = dbs.anidb_animes_raw.get(f"{lang}/{sid}.json")
        return TmdbJsonResponse(obj)
    except ObjectNotFound:
        raise HTTPException(status_code=404)


@router.get(
    "/images/{name}", 
    response_class=PersistedResponse,
    responses={
        "200": {
            "description": "Cached image file",
            "content": {"image/*": {}}
        },
        "404": {"description": "Image not found"}
    }
)
def get_image(name: str, dbs: Databases = Depends()):
    try:
        obj = dbs.tmdb_images.get(name)
        return PersistedResponse(obj)
    except ObjectNotFound:
        raise HTTPException(status_code=404)


@router.head(
    "/images/{name}", 
    response_class=PersistedStatResponse,
    responses={
        "200": {"description": "Cached image file"},
        "404": {"description": "Image not found"}
    }
)
def head_image(name: str, dbs: Databases = Depends()):
    try:
        obj = dbs.tmdb_images.stat(name)
        return PersistedStatResponse(obj)
    except ObjectNotFound:
        raise HTTPException(status_code=404)
