# -*- coding: UTF-8 -*-
import urllib.parse
import pathlib
import sys

from typing import Dict, Optional, Any, List, Tuple, Union


def get_plugin_params(argv: Optional[List[str]] = None) -> Dict[str, str]:
    argv = argv if argv else sys.argv
    qs = argv[2].lstrip('?')
    q = urllib.parse.parse_qs(qs)
    return {k: vl[0] for k, vl in q.items() if vl}


def get_plugin_handle(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv else sys.argv
    return int(sys.argv[1])


class URL:
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

