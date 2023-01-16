from fastapi import APIRouter, Depends, HTTPException

from amc2_api.model import Anime, combine_anime
from amc2_api.model import parse_anime_id, AnidbId, TmdbId, TmdbSeasonId, AnimeMappingId

from ..dependencies import Databases
from ..model import AnimeView, CollectionView, anime_link
from ..config import settings

from typing import List, Optional


router = APIRouter(
    prefix="/anime",
    tags=["anime"],
)


def load_anime(dbs: Databases, aid: str) -> Anime:
    anime: Optional[Anime] = None

    anime_id = parse_anime_id(aid)

    if isinstance(anime_id, AnidbId):
        if entry := dbs.anidb_animes.get(str(anime_id.anime)):
            anime = entry.anime
    elif isinstance(anime_id, TmdbId):
        if entry := dbs.tmdb_animes.get(str(anime_id.show)):
            anime = entry.anime
    elif isinstance(anime_id, TmdbSeasonId):
        if entry := dbs.tmdb_animes.get(str(anime_id.tvshow)):
            anime = entry.anime
    elif isinstance(anime_id, AnimeMappingId):
        anidb_entry = dbs.anidb_animes.get(str(anime_id.anidb.anime))
        tmdb_entry = dbs.tmdb_animes.get(str(anime_id.tmdb_show))
        if anidb_entry is None:
            raise HTTPException(status_code=404, detail="Anidb ID not found")
        if tmdb_entry is None:
            raise HTTPException(status_code=404, detail="Tmdb ID not found")
        anime = combine_anime(anidb_entry, tmdb_entry, anime_id.tmdb_season)
    else:
        raise HTTPException(status_code=404, detail="Invalid anime ID format")
    
    if anime is None:
        raise HTTPException(status_code=404)
    return anime


@router.get("/{anime_id}", response_model=AnimeView)
def get_anime(anime_id: str, dbs: Databases = Depends()):
    anime = load_anime(dbs, anime_id)
    view = AnimeView.from_model(anime, settings.self_base_url)
    view.links['anime'] = anime_link(settings.self_base_url, anime_id)
    return view

