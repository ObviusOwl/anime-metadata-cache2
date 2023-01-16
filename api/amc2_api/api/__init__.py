import logging
from fastapi import FastAPI

from .config import settings

logging.basicConfig(level=settings.logging_level.value)

from .routers.anidb import router as anidb_router
from .routers.tmdb import router as tmdb_router
from .routers.anime import router as anime_router
from .routers.match import router as match_router

# TODO: implement /find/ endpoints to find anime by title without matching

app = FastAPI()
app.include_router(anidb_router)
app.include_router(tmdb_router)
app.include_router(anime_router)
app.include_router(match_router)
