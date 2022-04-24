import datetime
import http
import json
from abc import abstractmethod
from typing import Union, List, Optional, Tuple

import requests

from ravendb import constants
from ravendb.http.misc import AggressiveCacheOptions, AggressiveCacheMode
from ravendb.http.request_executor import RequestExecutor
from ravendb.extensions.http_extensions import HttpExtensions
from ravendb.http.http_cache import HttpCache, ReleaseCacheItem
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import CaseInsensitiveDict


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
        self.values: Union[None, List[Tuple[ReleaseCacheItem, str]]] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self.values:
            return

        for value in self.values:
            if value is not None:
                value[0].close()

        self.values = None


class MultiGetCommand(RavenCommand):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_cache()

    def __init__(self, request_executor: RequestExecutor, commands: List[GetRequest]):
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

    def create_request(self, node: ServerNode) -> Optional[requests.Request]:
        self.__base_url = f"{node.url}/databases/{node.database}"
        url = self.__base_url + "/multi_get"
        # todo: aggressive caching
        # if self.__maybe_read_all_from_cache(self.__request_executor.aggressive_caching):
        #     self.aggressively_cached = True
        #     return None
        #
        # aggressive_cache_options: AggressiveCacheOptions = self.__request_executor.aggressive_caching
        # if aggressive_cache_options and aggressive_cache_options.mode == AggressiveCacheMode.TRACK_CHANGES:
        #     self.result = []
        #     for command in self.__commands:
        #         if not command.can_cache_aggressively:
        #             break
        #         cache_key = self.__get_cache_key(command)[0]
        #         cached_item, _, cached_ref = self.__http_cache.get(cache_key, "", "")
        #         cached_item: ReleaseCacheItem
        #         if (
        #             cached_ref is None
        #             or cached_item.age > aggressive_cache_options.duration
        #             or cached_item.might_have_been_modified
        #         ):
        #             break
        #         get_response = GetResponse()
        #         get_response.result = cached_ref
        #         get_response.status_code = http.HTTPStatus.NOT_MODIFIED
        #         self.result.append(get_response)
        #
        #     if len(self.result) == len(self.__commands):
        #         return None
        #
        #     self.result = None

        request = requests.Request("POST", url)

        request.data = {
            "Requests": [
                {
                    "Url": f"/databases/{node.database}{command.url}",
                    "Query": command.query,
                    "Method": command.method,
                    "Headers": command.headers,
                    "Content": command.content.write_content() if command.content else None,
                }
                for command in self.__commands
            ]
        }

        return request

    def __maybe_read_all_from_cache(self, options: AggressiveCacheOptions):
        self.close_cache()

        read_all_from_cache = options is not None
        track_changes = read_all_from_cache and options.mode == AggressiveCacheMode.TRACK_CHANGES
        for command in self.__commands:
            cache_key = self.__get_cache_key(command)[0]
            cached_item, change_vector, cached_ref = self.__http_cache.get(cache_key, "", "")
            cached_item: ReleaseCacheItem
            if cached_item.item is None:
                try:
                    read_all_from_cache = False
                    continue
                finally:
                    cached_item.close()

            if read_all_from_cache and (
                track_changes
                and cached_item.might_have_been_modified
                or cached_item.age > options.duration
                or not command.can_cache_aggressively
            ):
                read_all_from_cache = False

            command.headers[constants.Headers.IF_NONE_MATCH] = change_vector
            if self.__cached is None:
                self.__cached = Cached(len(self.__commands))

            self.__cached.values[self.__commands.index(command)] = (cached_item, cached_ref)

        if read_all_from_cache:
            with self.__cached as context:
                self.result = []
                for i in range(len(self.__commands)):
                    item_and_cached = self.__cached.values[i]
                    get_response = GetResponse()
                    get_response.result = item_and_cached[1]
                    get_response.status_code = http.HTTPStatus.NOT_MODIFIED

                    self.result.append(get_response)

            self.__cached = None

        return read_all_from_cache

    def __get_cache_key(self, command: GetRequest) -> (str, str):
        req_url = self.__base_url + command.url_and_query
        return command.method + "-" + req_url if command.method else req_url, req_url

    # todo: make sure json parses correctly down there
    def set_response_raw(self, response: requests.Response, stream: bytes) -> None:
        try:
            try:
                response_temp = response.json()
                if "Results" not in response_temp:
                    self._throw_invalid_response()

                i = 0
                self.result = []

                for get_response in self.read_responses(response_temp):
                    command = self.__commands[i]
                    self.__maybe_set_cache(get_response, command)

                    if self.__cached is not None and get_response.status_code == http.HTTPStatus.NOT_MODIFIED:
                        cloned_response = GetResponse()
                        cloned_response.result = self.__cached.values[i][1]
                        cloned_response.status_code = http.HTTPStatus.NOT_MODIFIED
                        self.result.append(cloned_response)
                    else:
                        self.result.append(get_response)

                    i += 1
            finally:
                if self.__cached is not None:
                    self.__cached.close()
        except Exception as e:
            self._throw_invalid_response(e)

    def __maybe_set_cache(self, get_response: GetResponse, command: GetRequest):
        if get_response.status_code == http.HTTPStatus.NOT_MODIFIED:
            return

        cache_key = self.__get_cache_key(command)[0]

        result = get_response.result
        if result == None:
            self.__http_cache.set_not_found(cache_key, self.aggressively_cached)
            return

        change_vector = HttpExtensions.get_etag_header(get_response.headers)
        if change_vector is None:
            return

        self.__http_cache.set(cache_key, change_vector, result)

    @staticmethod
    def read_responses(response_json: dict) -> List[GetResponse]:
        responses = []
        for response in response_json["Results"]:
            responses.append(MultiGetCommand.read_response(response))
        return responses

    @staticmethod
    def read_response(response_json: dict) -> GetResponse:
        get_response = GetResponse()
        get_response.result = json.dumps(response_json["Result"])  # todo: perf - redundant dump after response.json()
        get_response.headers = CaseInsensitiveDict(response_json["Headers"])
        if response_json["StatusCode"] == -1:
            MultiGetCommand._throw_invalid_response()
        get_response.status_code = response_json["StatusCode"]

        return get_response

    @property
    def is_read_request(self) -> bool:
        return False

    def close_cache(self):
        if self.__cached is not None:
            self.__cached.close()

        self.__cached = None
