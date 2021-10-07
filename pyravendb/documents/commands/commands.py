import hashlib
import http
import itertools
from typing import Optional, Union

import requests

from pyravendb import constants
from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.extensions.http_extensions import HttpExtensions
from pyravendb.http.http import ResponseDisposeHandling
from pyravendb.http.http_cache import HttpCache
from pyravendb.http.raven_command import RavenCommand
from pyravendb.http.server_node import ServerNode
from pyravendb.store.entity_to_json import EntityToJson
from pyravendb.tools.utils import Utils


class HeadDocumentCommand(RavenCommand):
    def __init__(self, key: str, change_vector: str):
        super(HeadDocumentCommand, self).__init__(result_class=str)
        if key is None:
            raise ValueError("Key cannot be None")
        self.__key = key
        self.__change_vector = change_vector

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> (requests.Request, str):
        url = f"{node.url}/databases/{node.database}/docs?id{Utils.escape(self.__key,True,False)}"
        request = requests.Request("HEAD")
        if self.__change_vector is not None:
            request.headers["If-None-Match"] = self.__change_vector
        return request, url

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
        if http.HTTPStatus.NOT_MODIFIED == response.status_code:
            self.result = self.__change_vector
            return ResponseDisposeHandling.AUTOMATIC

        if response.status_code == http.HTTPStatus.NOT_FOUND:
            self.result = None
            return ResponseDisposeHandling.AUTOMATIC

        self.result = HttpExtensions.get_required_etag_header(response)
        return ResponseDisposeHandling.AUTOMATIC


class GetDocumentsCommand(RavenCommand):
    class GetDocumentsCommandOptionsBase:
        def __init__(
            self,
            counter_includes: Optional[list[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: DocumentConventions = None,
        ):
            self.counter_includes = counter_includes
            self.include_all_counters = include_all_counters
            self.conventions = conventions

    class GetDocumentsByIdCommandOptions(GetDocumentsCommandOptionsBase):
        def __init__(
            self,
            key: str,
            includes: list[str],
            metadata_only: bool,
            counter_includes: Optional[list[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: DocumentConventions = None,
        ):
            super().__init__(counter_includes, include_all_counters, conventions)
            self.key = key
            self.includes = includes
            self.metadata_only = metadata_only

    class GetDocumentsByIdsCommandOptions(GetDocumentsCommandOptionsBase):
        def __init__(
            self,
            keys: list[str],
            includes: Optional[list[str]] = None,
            metadata_only: Optional[bool] = None,
            time_series_includes: Optional[list[TimeSeriesRange]] = None,
            compare_exchange_value_includes: Optional[list[str]] = None,
            counter_includes: Optional[list[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: DocumentConventions = None,
        ):
            super().__init__(counter_includes, include_all_counters, conventions)
            self.keys = keys
            self.includes = includes
            self.metadata_only = metadata_only
            self.time_series_includes = time_series_includes
            self.compare_exchange_value_includes = compare_exchange_value_includes

    class GetDocumentsStartingWithCommandOptions(GetDocumentsCommandOptionsBase):
        def __init__(
            self,
            start: int,
            page_size: int,
            starts_with: Optional[str] = None,
            start_after: Optional[str] = None,
            matches: Optional[str] = None,
            exclude: Optional[str] = None,
            metadata_only: Optional[bool] = None,
            counter_includes: Optional[list[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: DocumentConventions = None,
        ):
            super().__init__(counter_includes, include_all_counters, conventions)
            self.start = start
            self.page_size = page_size
            self.starts_with = starts_with
            self.start_after = start_after
            self.matches = matches
            self.exclude = exclude
            self.metadata_only = metadata_only

    def __init__(self, options: GetDocumentsCommandOptionsBase):
        super().__init__(GetDocumentsResult)
        self.__options = options
        if isinstance(options, GetDocumentsCommand.GetDocumentsByIdCommandOptions):
            pass
        elif isinstance(options, GetDocumentsCommand.GetDocumentsByIdsCommandOptions):
            pass
        elif isinstance(options, GetDocumentsCommand.GetDocumentsStartingWithCommandOptions):
            pass
        else:
            raise TypeError("Unexpected options type")

    def create_request(self, node: ServerNode) -> (requests.Request, str):
        path_builder: list[str] = [node.url, f"/databases/{node.database}/docs?"]
        if "start" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&start={self.__options.start}")

        if "page_size" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&pageSize={self.__options.page_size}")

        if "metadata_only" in self.__options.__dict__ and self.__options.metadata_only:
            path_builder.append("&metadataOnly=true")

        if "starts_with" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&start={self.__options.starts_with}")

            if "matches" in self.__options.__dict__:
                path_builder.append(f"&matches={self.__options.matches}")
            if "exclude" in self.__options.__dict__:
                path_builder.append(f"&exclude={self.__options.exclude}")
            if "start_after" in self.__options.__dict__:
                path_builder.append(f"&startAfter={self.__options.start_after}")

        if "includes" in self.__options.__dict__:
            self.__options: Union[
                GetDocumentsCommand.GetDocumentsByIdCommandOptions, GetDocumentsCommand.GetDocumentsByIdsCommandOptions
            ]
            for include in self.__options.includes:
                path_builder.append(f"&include{include}")

        if self.__options.include_all_counters:
            path_builder.append(f"&counter={constants.Counters.ALL}")
        elif self.__options.counter_includes:
            for counter in self.__options.counter_includes:
                path_builder.append(f"counter={counter}")

        if "time_series_includes" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsByIdsCommandOptions
            for time_series in self.__options.time_series_includes:
                path_builder.append(
                    f"&timeseries={time_series.name}&from={time_series.from_date}&to={time_series.to_date}"
                )

        if "compare_exchange_value_includes" in self.__options.__dict__:
            for compare_exchange_value in self.__options.compare_exchange_value_includes:
                path_builder.append(f"&cmpxchg={compare_exchange_value}")

        request = requests.Request("GET", "".join(path_builder))

        if "key" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsByIdCommandOptions
            request.url += f"&id={self.__options.key}"
        elif "keys" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsByIdsCommandOptions

        return request

    def prepare_request_with_multiple_ids(
        self, path_builder: list[str], request: requests.Request, ids: list[str]
    ) -> requests.Request:
        unique_ids = set()
        unique_ids.update(ids)
        is_get = sum(map(len, list(filter(lambda x: x, unique_ids))), int.__add__) < 1024

        if is_get:
            for key in unique_ids:
                path_builder.append(f"&id={Utils.escape(key,False,False) if key else ''}")

            return requests.Request("GET", "".join(path_builder))

        else:
            http_post = requests.Request("POST")
            try:
                calculate_hash = self.__calculate_hash(unique_ids)
                path_builder.append(f"&loadHash={calculate_hash}")
            except IOError as e:
                raise RuntimeError(f"Unable to computer query hash: {e.args[0]}")

            http_post.data = {"Ids": [str(key) for key in unique_ids]}
            return http_post

    @staticmethod
    def __calculate_hash(unique_ids: set[str]) -> str:
        return hashlib.md5("".join(unique_ids)).hexdigest()

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = None
            return

        self.result = EntityToJson.convert_to_entity_static(
            response, self.response_type, self.__options.conventions, None, None
        )
