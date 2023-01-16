from fastapi import APIRouter, Depends, Response, HTTPException

from amc2_api.model import Title
from amc2_api.model import parse_anime_id, AnidbId, TmdbId, TmdbSeasonId, AnimeMappingId
from amc2_api.mapping import AnimeMapping, AnidbTitleMatcher

from ..dependencies import Databases
from ..model import TitleMappingView, CollectionView, AnimeMappingView
from ..config import settings

from typing import List, Optional


router = APIRouter(
    prefix="/match",
    tags=["anime", 'match'],
)


def match_id(anime_id: str) -> AnimeMappingId:
    try:
        return AnimeMappingId.parse(anime_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Invalid anime ID")


@router.get("/", response_model=CollectionView[TitleMappingView])
def find_match(
    title: str, 
    db: str = 'anidb', 
    dbs: Databases = Depends()
) -> None:

    if not title:
        raise HTTPException(status_code=400, detail='title must not be empty')
    if db != 'anidb':
        # TODO: implement tmdb title search
        raise HTTPException(status_code=400, detail='Can only match against anidb database')

    query = Title(value=title)
    matcher = AnidbTitleMatcher(
        dbs.anidb_titles, 
        dbs.tmdb_titles, 
        dbs.anime_mapping
    )
    matches = matcher.match_title(query)

    items: List[TitleMappingView] = []
    for match in matches:
        items.append(TitleMappingView.from_model(match, settings.self_base_url))
    return CollectionView(items=items)


@router.get("/{anime_id}", response_model=AnimeMappingView)
def get_match(anime_id: AnimeMappingId = Depends(match_id), dbs: Databases = Depends()) -> None:
    query = AnimeMapping(anidb=str(anime_id.anidb.anime), tmdb=str(anime_id.tmdb))
    match = dbs.anime_mapping.load(query)

    if match is None:
        raise HTTPException(status_code=404)
    return AnimeMappingView.from_model(match, settings.self_base_url)


@router.put("/{anime_id}")
def store_match(anime_id: AnimeMappingId = Depends(match_id), dbs: Databases = Depends()) -> None:
    query = AnimeMapping(anidb=str(anime_id.anidb.anime), tmdb=str(anime_id.tmdb))
    if dbs.anime_mapping.load(query) is None:
        # make put idempotent (does not trigger a useless save to disk)
        dbs.anime_mapping.store([query], replace=True)


@router.delete("/{anime_id}")
def delete_match(anime_id: AnimeMappingId = Depends(match_id), dbs: Databases = Depends()) -> None:
    query = AnimeMapping(anidb=str(anime_id.anidb.anime), tmdb=str(anime_id.tmdb))
    if dbs.anime_mapping.load(query) is not None:
        # delete is idempotent
        dbs.anime_mapping.remove(query)
