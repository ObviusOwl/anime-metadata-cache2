import logging
import email.utils

import requests

from amc2_api.utils import Throttler, parse_mime, URL

from .model import Persisted, PersistedStat, ObjectNotFound, WriteNotSupported

from typing import List, Optional, Dict, Union, Tuple, Mapping, Callable

_logger = logging.getLogger(__name__)


class MaybeThrottler:
    throttler: Optional[Throttler]

    def __init__(self, interval: float) -> None:
        if interval > 0:
            self.throttler = Throttler(interval)
        else:
            self.throttler = None

    def reset(self):
        if self.throttler is not None:
            self.throttler.reset()

    def mark(self):
        if self.throttler is not None:
            self.throttler.mark()
    
    def check(self) -> bool:
        if self.throttler is not None:
            return self.throttler.check()
        return True

    def wait(self):
        if self.throttler is not None:
            self.throttler.wait()



class HTTPObjectStore:
    """
    Abstract base class for implementing an object store using the HTTP protocol.
    """
    user_agent: str = ''

    _req_throttler: MaybeThrottler
    _err_throttler: MaybeThrottler
    _default_headers: Dict[str, str]

    def __init__(
        self, 
        req_interval: float = -1, 
        err_interval: float = -1,
    ) -> None:
        self._req_throttler = MaybeThrottler(req_interval)
        self._err_throttler = MaybeThrottler(err_interval)

        self._default_headers = {}
        if self.user_agent:
            self._default_headers['User-Agent'] = self.user_agent

    def _combine_headers(self, headers: Dict[str, str], top: Dict[str, str]) -> Dict[str,str]:
        headers = headers.copy()
        for name, value in top.items():
            if name in headers and not value:
                del headers[name]
            elif name:
                headers[name] = value
        return headers

    def _http(
        self, 
        verb: str, 
        url: Union[URL, str], 
        headers: Optional[Dict[str, str]] = None, 
        http_errors: Union[bool, Callable[[requests.Response], None]] = True,
    ) -> requests.Response:

        # don't flood the API when there are errors, one error never comes alone
        if not self._err_throttler.check():
            raise ObjectNotFound("Too many requests after the last error")
        
        # make sure we stay in the given request rate limit by serializing
        self._req_throttler.wait()

        if headers is None:
            headers = {}

        response = requests.request(
            verb.upper(),
            str(url),
            headers=self._combine_headers(self._default_headers, headers),
            allow_redirects=True
        )

        if response.ok:
            # we recovered from the error or no error ever happened
            self._err_throttler.reset()
        else:
            # we assume 404 errors are normal
            if response.status_code != 404:
                self._err_throttler.mark()

            # call the user provided error handler or the default to raise the exception
            if callable(http_errors):
                http_errors(response)
            elif http_errors:
                self._handle_http_errors(response)
            
        return response

    def _handle_http_errors(self, resp: requests.Response) -> None:
        if resp.status_code == 404:
            raise ObjectNotFound()
        elif not resp.ok:
            raise ObjectNotFound(f"Unexpected HTTP {resp.status_code} Error: {resp.reason}")

    def _parse_last_modified(self, response: requests.Response) -> Optional[float]:
        value = response.headers.get('last-modified', '')
        if not value:
            return None
        
        try:
            return email.utils.parsedate_to_datetime(value).timestamp()
        except ValueError as e:
            _logger.warning("Error parsing last-midified header: " + str(e))
            return None

    def _make_url(self, name: str, stat: bool) -> str:
        raise NotImplementedError()

    def _make_headers(self, name: str, stat: bool) -> Dict[str, str]:
        return {}
    
    def _make_content(self, name: str, response: requests.Response) -> bytes:
        # may be overridden to modify the content
        return response.content

    def _make_stat(self, name: str, response: requests.Response) -> PersistedStat:
        mime = response.headers['content-type']
        mtime = self._parse_last_modified(response)
        size = int(response.headers.get('content-length', '0'))
        return PersistedStat(
            content_type=mime,
            last_modified=mtime,
            size=size
        )

    def _make_persisted(self, name: str, response: requests.Response) -> Persisted:
        mime = response.headers['content-type']
        mtime = self._parse_last_modified(response)
        content = self._make_content(name, response)
        return Persisted(
            content_type=mime, 
            last_modified=mtime,
            data=content
        )

    def stat(self, name: str) -> PersistedStat:
        try:
            url = self._make_url(name, stat=True)
            headers = self._make_headers(name, stat=True)
            response = self._http('HEAD', url, headers=headers)
            return self._make_stat(name, response)
        except ObjectNotFound as e:
            e.object_name = name
            raise e

    def get(self, name: str) -> Persisted:
        try:
            url = self._make_url(name, stat=False)
            headers = self._make_headers(name, stat=False)
            response = self._http('GET', url, headers=headers)
            return self._make_persisted(name, response)
        except ObjectNotFound as e:
            e.object_name = name
            raise e

    def put(self, name: str, obj: Persisted) -> None:
        raise WriteNotSupported("Upload to HTTP not implemented")
