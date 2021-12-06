from __future__ import annotations
import datetime
import http
import json
from typing import Optional, Union, List
from typing import TYPE_CHECKING
import requests

from pyravendb import constants
from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.documents.queries import HashCalculator
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.identity import HiLoResult
from pyravendb.extensions.http_extensions import HttpExtensions
from pyravendb.http import ResponseDisposeHandling, VoidRavenCommand
from pyravendb.http.http_cache import HttpCache
from pyravendb.http.raven_command import RavenCommand
from pyravendb.http.server_node import ServerNode
from pyravendb.tools.utils import Utils


if TYPE_CHECKING:
    from pyravendb.data.document_conventions import DocumentConventions


class PutResult:
    def __init__(self, key: Optional[str] = None, change_vector: Optional[str] = None):
        self.key = key
        self.change_vector = change_vector

    @staticmethod
    def from_json(json_dict: dict) -> PutResult:
        return PutResult(json_dict["Id"], json_dict["ChangeVector"])


class PutDocumentCommand(RavenCommand[PutResult]):
    def __init__(self, key: str, change_vector: str, document: dict):
        if not key:
            raise ValueError("Key cannot be None")

        if document is None:
            raise ValueError("Document cannot be None")

        super().__init__(PutResult)
        self.__key = key
        self.__change_vector = change_vector
        self.__document = document

    def create_request(self, node: ServerNode) -> requests.Request:
        request = requests.Request("PUT", f"{node.url}/databases/{node.database}/docs?id={self.__key}")
        request.data = json.loads(json.dumps(self.__document))
        self._add_change_vector_if_not_none(self.__change_vector, request)
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        self.result = PutResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return False


class DeleteDocumentCommand(VoidRavenCommand):
    def __init__(self, key: str, change_vector: Optional[str] = None):
        if not key:
            raise ValueError("Key cannot be None")
        super().__init__()
        self.__key = key
        self.__change_vector = change_vector

    def create_request(self, node: ServerNode) -> requests.Request:
        self.ensure_is_not_null_or_string(self.__key, "id")
        request = requests.Request(
            "DELETE", f"{node.url}/databases/{node.database}/docs?id={Utils.escape(self.__key, None,None)}"
        )
        self._add_change_vector_if_not_none(self.__change_vector, request)
        return request


class HeadDocumentCommand(RavenCommand[str]):
    def __init__(self, key: str, change_vector: str):
        super(HeadDocumentCommand, self).__init__(str)
        if key is None:
            raise ValueError("Key cannot be None")
        self.__key = key
        self.__change_vector = change_vector

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/docs?id={Utils.escape(self.__key,True,False)}"
        request = requests.Request("HEAD", url)
        if self.__change_vector is not None:
            request.headers["If-None-Match"] = self.__change_vector
        return request

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
        if http.HTTPStatus.NOT_MODIFIED == response.status_code:
            self.result = self.__change_vector
            return ResponseDisposeHandling.AUTOMATIC

        if response.status_code == http.HTTPStatus.NOT_FOUND:
            self.result = None
            return ResponseDisposeHandling.AUTOMATIC

        self.result = HttpExtensions.get_required_etag_header(response)
        return ResponseDisposeHandling.AUTOMATIC

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is not None:
            raise ValueError("Invalid response")

        self.result = None


class GetDocumentsCommand(RavenCommand):
    class GetDocumentsCommandOptionsBase:
        def __init__(
            self,
            counter_includes: Optional[List[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: "DocumentConventions" = None,
        ):
            self.counter_includes = counter_includes
            self.include_all_counters = include_all_counters
            self.conventions = conventions

    class GetDocumentsByIdCommandOptions(GetDocumentsCommandOptionsBase):
        def __init__(
            self,
            key: str,
            includes: Optional[List[str]] = None,
            metadata_only: Optional[bool] = False,
            counter_includes: Optional[List[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: "DocumentConventions" = None,
        ):
            super().__init__(counter_includes, include_all_counters, conventions)
            self.key = key
            self.includes = includes
            self.metadata_only = metadata_only

    class GetDocumentsByIdsCommandOptions(GetDocumentsCommandOptionsBase):
        def __init__(
            self,
            keys: List[str],
            includes: Optional[List[str]] = None,
            metadata_only: Optional[bool] = None,
            time_series_includes: Optional[List[TimeSeriesRange]] = None,
            compare_exchange_value_includes: Optional[List[str]] = None,
            counter_includes: Optional[List[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: "DocumentConventions" = None,
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
            counter_includes: Optional[List[str]] = None,
            include_all_counters: Optional[bool] = None,
            conventions: "DocumentConventions" = None,
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

    @property
    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> (requests.Request):
        path_builder: List[str] = [node.url, f"/databases/{node.database}/docs?"]
        if "start" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&start={self.__options.start}")

        if "page_size" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&pageSize={self.__options.page_size}")

        if "metadata_only" in self.__options.__dict__ and self.__options.metadata_only:
            path_builder.append("&metadataOnly=true")

        if "starts_with" in self.__options.__dict__ and self.__options.starts_with:
            self.__options: GetDocumentsCommand.GetDocumentsStartingWithCommandOptions
            path_builder.append(f"&start={self.__options.starts_with}")

            if "matches" in self.__options.__dict__ and self.__options.matches:
                path_builder.append(f"&matches={self.__options.matches}")
            if "exclude" in self.__options.__dict__ and self.__options.exclude:
                path_builder.append(f"&exclude={self.__options.exclude}")
            if "start_after" in self.__options.__dict__ and self.__options.start_after:
                path_builder.append(f"&startAfter={self.__options.start_after}")

        if "includes" in self.__options.__dict__ and self.__options.includes:
            self.__options: Union[
                GetDocumentsCommand.GetDocumentsByIdCommandOptions, GetDocumentsCommand.GetDocumentsByIdsCommandOptions
            ]
            for include in self.__options.includes:
                path_builder.append(f"&include={include}")

        if self.__options.include_all_counters:
            path_builder.append(f"&counter={constants.Counters.ALL}")
        elif self.__options.counter_includes:
            for counter in self.__options.counter_includes:
                path_builder.append(f"counter={counter}")

        if "time_series_includes" in self.__options.__dict__ and self.__options.time_series_includes:
            self.__options: GetDocumentsCommand.GetDocumentsByIdsCommandOptions
            for time_series in self.__options.time_series_includes:
                path_builder.append(
                    f"&timeseries={time_series.name}&from={time_series.from_date}&to={time_series.to_date}"
                )

        if (
            "compare_exchange_value_includes" in self.__options.__dict__
            and self.__options.compare_exchange_value_includes
        ):
            for compare_exchange_value in self.__options.compare_exchange_value_includes:
                path_builder.append(f"&cmpxchg={compare_exchange_value}")

        request = requests.Request("GET", "".join(path_builder))

        if "key" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsByIdCommandOptions
            request.url += f"&id={self.__options.key}"
        elif "keys" in self.__options.__dict__:
            self.__options: GetDocumentsCommand.GetDocumentsByIdsCommandOptions
            request = self.prepare_request_with_multiple_ids(path_builder, request, self.__options.keys)

        return request

    def prepare_request_with_multiple_ids(
        self, path_builder: List[str], request: requests.Request, ids: List[str]
    ) -> requests.Request:
        unique_ids = set()
        unique_ids.update(ids)
        is_get = sum(map(len, list(filter(lambda x: x, unique_ids)))) < 1024

        if is_get:
            for key in unique_ids:
                path_builder.append(f"&id={key if key else ''}")  # todo: check if need to escape at this point

            return requests.Request("GET", "".join(path_builder))

        else:
            http_post = requests.Request("POST")
            try:
                calculate_hash = HashCalculator.calculate_hash_from_str_collection(unique_ids)
                path_builder.append(f"&loadHash={calculate_hash}")
            except IOError as e:
                raise RuntimeError(f"Unable to computer query hash: {e.args[0]}")

            http_post.data = {"Ids": [str(key) for key in unique_ids]}
            http_post.url = "".join(path_builder)
            return http_post

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = None
            return
        self.result = GetDocumentsResult.from_json(json.loads(response))


class NextHiLoCommand(RavenCommand[HiLoResult]):
    def __init__(
        self,
        tag: str,
        last_batch_size: int,
        last_range_at: datetime.datetime,
        identity_parts_separator: str,
        last_range_max: int,
    ):
        super().__init__(HiLoResult)
        self.__tag = tag
        self.__last_batch_size = last_batch_size
        self.__last_range_at = last_range_at
        self.__identity_parts_separator = identity_parts_separator
        self.__last_range_max = last_range_max

    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> requests.Request:
        date = Utils.datetime_to_string(self.__last_range_at) if self.__last_range_at else ""
        # todo: check escapeUriString vs escapeDataString difference - maybe write escape for uri
        path = (
            f"/hilo/next?tag={Utils.escape(self.__tag,None,None)}"
            f"&lastBatchSize={self.__last_batch_size if self.__last_batch_size is not None else 0}"
            f"&lastRangeAt={date}"
            f"&identityPartsSeparator={Utils.escape(self.__identity_parts_separator,True,False)}"
            f"&lastMax={self.__last_range_max}"
        )

        url = f"{node.url}/databases/{node.database}{path}"
        return requests.Request(method="GET", url=url)

    def set_response(self, response: str, from_cache: bool) -> None:
        self.result = HiLoResult.from_json(json.loads(response))


class HiLoReturnCommand(VoidRavenCommand):
    def __init__(self, tag: str, last: int, end: int):
        super().__init__()
        if last < 0:
            raise ValueError("Last is < 0")

        if end < 0:
            raise ValueError("End is <0")

        if tag is None:
            raise ValueError("tag cannot be None")

        self.__tag = tag
        self.__last = last
        self.__end = end

    def create_request(self, node: ServerNode) -> (requests.Request):
        return requests.Request(
            "PUT",
            url=f"{node.url}/databases/{node.database}/hilo/return?tag={self.__tag}&end={self.__end}&last={self.__last}",
        )
