import datetime
import http
from abc import abstractmethod
from typing import Union

import requests

from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.http.http import AggressiveCacheMode, AggressiveCacheOptions
from pyravendb.http.http_cache import HttpCache, ReleaseCacheItem
from pyravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from pyravendb.http.server_node import ServerNode
from pyravendb.tools.utils import CaseInsensitiveDict


class Content:
    @abstractmethod
    def write_content(self) -> dict:
        pass


class GetRequest:
    def __init__(self):
        self.headers = CaseInsensitiveDict()
        self.url: Union[None, str] = None
        self.query: Union[None, str] = None
        self.method: Union[None, str] = None
        self.content: Union[None, Content] = None
        self.can_cache_aggressively = True

    @property
    def url_and_query(self) -> str:
        if not self.query:
            return self.url
        if self.query.startswith("?"):
            return self.url + self.query
        return self.url + "?" + self.query


class GetResponse:
    def __init__(self):
        self.headers = CaseInsensitiveDict()
        self.elapsed: Union[None, datetime.timedelta] = None
        self.result: Union[None, str] = None
        self.status_code: Union[None, int] = None
        self.force_retry: Union[None, bool] = None

    @property
    def request_has_errors(self):
        return self.status_code not in [
            0,
            http.HTTPStatus.OK,
            http.HTTPStatus.CREATED,
            http.HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
            http.HTTPStatus.NO_CONTENT,
            http.HTTPStatus.NOT_MODIFIED,
            http.HTTPStatus.NOT_FOUND,
        ]


class Cached:
    def __init__(self, size: int):
        self.__size = size
        self.__values: Union[None, list[tuple[ReleaseCacheItem, str]]] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self.__values:
            return

        for value in self.__values:
            if value is not None:
                value[0].close()

        self.__values = None


class MultiGetCommand(RavenCommand):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_cache()

    def __init__(self, request_executor: RequestsExecutor, commands: list[GetRequest]):
        super(MultiGetCommand, self).__init__(list)

        if not request_executor:
            raise ValueError("RequestExecutor cannot be None")

        cache = request_executor.cache

        if cache is None:
            raise ValueError("Cache cannot be None")

        if commands is None:
            raise ValueError("Command cannot be None")

        self.__request_executor = request_executor
        self.__http_cache: HttpCache = request_executor.cache
        self.__commands = commands
        self._response_type = RavenCommandResponseType.RAW
        self.aggressively_cached: Union[None, bool] = None
        self.__base_url: str = ""
        self.__cached: Union[None, Cached] = None

    def create_request(self, node: ServerNode) -> (requests.Request, str):
        self.__base_url = f"{node.url}/databases/{node.database}"
        url = self.__base_url + "/multi_get"
        if self.maybe_read_all_from_cache(self.__request_executor.aggressive_caching):
            self.aggressively_cached = True
            return None

        url = self.__base_url + "/multi_get"
        aggressive_cache_options: AggressiveCacheOptions = self.__request_executor.aggressive_caching
        if aggressive_cache_options is not None and aggressive_cache_options.mode == AggressiveCacheMode.TRACK_CHANGES:
            self.result = []
            for command in self.__commands:
                if not command.can_cache_aggressively:
                    break
                cache_key = self.__get_cache_key(command)[0]
                cached_item, _, cached_ref = self.__http_cache.get(cache_key, "", "")
                cached_item: ReleaseCacheItem
                if (
                    cached_ref is None
                    or cached_item.age > aggressive_cache_options.duration
                    or cached_item.might_have_been_modified
                ):
                    break
                get_response = GetResponse()
                get_response.result = cached_ref
                get_response.status_code = http.HTTPStatus.NOT_MODIFIED
                self.result.append(get_response)

            if len(self.result) == len(self.__commands):
                return None

            self.result = None

        request = requests.Request("POST")

        request.data = {
            "Requests": [
                {
                    "Url": f"/databases/{node.database}{command.url}",
                    "Query": command.query,
                    "Method": command.method,
                    "Headers": command.headers,
                    "Content": command.content.write_content(),
                }
                for command in self.__commands
            ]
        }

        return request

    def __maybe_read_all_from_cache(self, options: AggressiveCacheOptions):
        self.close_cache()

        read_all_from_cache = options != None
        track_changes = read_all_from_cache and options.mode == AggressiveCacheMode.TRACK_CHANGES
        for command in self.__commands:
            cache_key = self.__get_cache_key(command)[0]
            cached_item, change_vector, cached_ref = self.__http_cache.get(cache_key, "", "")

    def __get_cache_key(self, command: GetRequest) -> (str, str):
        req_url = self.__base_url + command.url_and_query
        return command.method + "-" + req_url if command.method else req_url, req_url

    def close_cache(self):
        if self.__cached is not None:
            self.__cached.close()

        self.__cached = None
