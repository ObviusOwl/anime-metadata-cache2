from fastapi import APIRouter, Depends, HTTPException

from amc2_api.persistence import ObjectNotFound

from ..dependencies import Databases
from ..responses import PersistedResponse, PersistedStatResponse
from ..config import settings


class AnidbXMLResponse(PersistedResponse):
    media_type = 'text/xml'
    charset = "utf-8"


router = APIRouter(
    prefix="/anidb",
    tags=["anidb"]
)


@router.get(
    "/shows/{aid}", 
    response_class=AnidbXMLResponse,
    responses={
        "200": {"description": "Unchanged but cached Anidb anime XML"},
        "404": {"description": "Anidb ID not found"}
    }
)
def get_show(aid: str, dbs: Databases = Depends()):
    try:
        obj = dbs.anidb_animes_raw.get(f"{aid}.xml")
        return AnidbXMLResponse(obj)
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
        obj = dbs.anidb_images.get(name)
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
        obj = dbs.anidb_images.stat(name)
        return PersistedStatResponse(obj)
    except ObjectNotFound:
        raise HTTPException(status_code=404)
