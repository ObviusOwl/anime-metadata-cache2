import email.utils
from starlette.background import BackgroundTask
from fastapi import Response, HTTPException

from amc2_api.persistence import Persisted, PersistedStat

from typing import Optional, Mapping


class PersistedResponse(Response):
    def __init__(
        self,
        content: Persisted,
        status_code: int = 200,
        headers: Optional[Mapping[str, str]] = None,
        media_type: Optional[str] = None,
        background: Optional[BackgroundTask] = None,
    ) -> None:

        if media_type is None and self.media_type is None:
            media_type = content.content_type

        mtime = email.utils.formatdate(content.last_modified, usegmt=True)
        if headers is None:
            headers = {'last-modified': mtime}
        else:
            headers['last-modified'] = mtime

        super().__init__(
            content=content.data,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background
        )


class PersistedStatResponse(Response):
    def __init__(
        self,
        content: PersistedStat,
        status_code: int = 200,
        headers: Optional[Mapping[str, str]] = None,
        media_type: Optional[str] = None,
        background: Optional[BackgroundTask] = None,
    ) -> None:
        if media_type is None and self.media_type is None:
            media_type = content.content_type

        mtime = email.utils.formatdate(content.last_modified, usegmt=True)
        if headers is None:
            headers = {'last-modified': mtime}
        else:
            headers['last-modified'] = mtime
        headers['content-length'] = str(content.size)

        super().__init__(
            content=None,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background
        )

