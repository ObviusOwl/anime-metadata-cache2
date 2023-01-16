import time
import datetime
import threading
import urllib.parse
import pathlib

from typing import Union, Tuple, Dict, Optional, List


__all__ = [
    'Throttler',
    'parse_mime',
    'URL',
    'parse_timedelta',
]

class Throttler:
    _time: Optional[float]
    _interval: float
    _lock: threading.RLock

    def __init__(self, interval: float) -> None:
        if interval <= 0:
            raise ValueError("Throttle interval must be strictly positive")
        self._time = None
        self._interval = float(interval)
        self._lock = threading.RLock()
    
    @property
    def interval(self) -> float:
        return self._interval
    
    @property
    def timedelta(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=self._interval)
    
    def reset(self):
        self._time = None
    
    def mark(self):
        # don't block acquiring the lock
        self._time = time.monotonic()
    
    def check(self) -> bool:
        # returns True if it is allowed
        if self._time is None:
            return True
        return (time.monotonic() - self._time) > self._interval

    # use the wait method between two calls when accessing the API
    def wait(self):
        if self._time is None:
            return
        with self._lock:
            delta = time.monotonic() - self._time
            if delta < self._interval:
                # sleep while holding the lock to make sure the execution is serialized
                time.sleep(delta)
                # the next one getting the lock needs to wait the full time
                self.mark()


def parse_mime(value: str) -> Tuple[str, str, str]:
    x = value.split(';')
    y = x[0].split('/')
    return (y[0], y[1], x[1] if len(x) > 1 else '')


class URL:
    """
    Does not support multiple values for a key. While allowed in URLs, it is 
    not used very often and makes implementation/use too complicated.
    """

    scheme: str
    netloc = str
    path: str
    params: str
    query: Dict[str, str]
    fragment: str
    username: Optional[str]
    password: Optional[str]
    hostname: Optional[str]
    port: Optional[int]

    def __init__(self, url: Union[str, 'URL']) -> None:
        if isinstance(url, URL):
            self.scheme = url.scheme
            self.path = url.path
            self.params = url.params
            self.query_string = url.query_string
            self.fragment = url.fragment
            self.username = url.username
            self.password = url.password
            self.hostname = url.hostname
            self.port = url.port
        else:
            tup = urllib.parse.urlparse(url)
            self.scheme = tup.scheme
            self.path = tup.path
            self.params = tup.params
            self.query_string = tup.query
            self.fragment = tup.fragment
            self.username = tup.username
            self.password = tup.password
            self.hostname = tup.hostname
            self.port = tup.port
    
    def copy(self) -> 'URL':
        return URL(self)

    @property
    def userinfo(self) -> str:
        userinfo = ''
        if self.username is not None or self.password is not None:
            userinfo = ':'
        if self.username is not None:
            userinfo = self.username + userinfo
        if self.password is not None:
            userinfo = userinfo + self.password
        return userinfo

    @property
    def netloc(self) -> str:
        # authority = [userinfo "@"] host [":" port]
        userinfo = self.userinfo + '@' if self.userinfo else ''
        port = ':' + str(self.port) if self.port is not None else ''
        host = self.hostname if self.hostname else ''
        return userinfo + host + port
    
    @property
    def query_string(self) -> str:
        return urllib.parse.urlencode(self.query)
    
    @query_string.setter
    def query_string(self, value: str) -> None:
        q = urllib.parse.parse_qs(value)
        self.query = {k: vl[0] for k, vl in q.items() if vl}
    
    def path_parts(self) -> List[str]:
        return list(pathlib.PurePath(self.path).parts)

    def append_path(self, *parts: str) -> None:
        self.path = str(pathlib.PurePath(self.path).joinpath(*parts))

    def joinpath(self, *parts: str) -> 'URL':
        url = URL(self)
        url.append_path(*parts)
        return url
    
    def with_qs(self, **qs: str) -> 'URL':
        url = URL(self)
        url.query.update(qs)
        return url

    def __str__(self) -> str:
        tup = (self.scheme, self.netloc, self.path, self.params, self.query_string, self.fragment)
        return urllib.parse.urlunparse(tup)


class TimedeltaParser:
    whitespace: List[str]
    factors: Dict[str, int]

    def __init__(self):
        self.whitespace = [' ', '\n', '\t']
        self.factors = {
            's': 1, 
            'min': 60, 
            'h': 60*60,
            'd': 24*60*60,
            'w': 7*24*60*60,
            'mo': 30*24*60*60,
            'y': 365*24*60*60,
        }

    def tokenize(self, value: str) -> List[str]:
        value = value.lower()
        tokens: List[str] = []
        old_st = 0
        
        for c in value:
            if c.isdecimal():
                new_st = 1
            elif c in self.whitespace:
                new_st = 0
            else:
                new_st = 2
            
            if not tokens or (new_st != old_st and old_st != 0):
                tokens.append('')

            if new_st != 0:
                tokens[-1] += c
            old_st = new_st
    
        return tokens
    
    def parse(self, tokens: List[str]) -> int:
        seconds = 0
        num = 0
        
        for i, tok in enumerate(tokens):
            if i % 2 == 0:
                try:
                    num = int(tok)
                except ValueError:
                    raise ValueError(f"Expected digits, got '{tok}'") from None
            else:
                if not tok in self.factors:
                    choices = '|'.join(self.factors)
                    raise ValueError(f"Expected one of {choices}, got '{tok}'")
                seconds += self.factors[tok] * num

        return seconds

    
    def __call__(self, value: str) -> int:
        tokens = self.tokenize(value)
        if not tokens:
            return 0
        return self.parse(tokens)
    

parse_timedelta = TimedeltaParser()
