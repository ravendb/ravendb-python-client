from __future__ import annotations
import datetime
import http
import json
from typing import Optional, List
from typing import TYPE_CHECKING
import requests

from ravendb import constants
from ravendb.documents.commands.results import GetDocumentsResult
from ravendb.documents.queries.utils import HashCalculator
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.documents.identity.hilo import HiLoResult
from ravendb.extensions.http_extensions import HttpExtensions
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class PutResult:
    def __init__(self, key: Optional[str] = None, change_vector: Optional[str] = None):
        self.key = key
        self.change_vector = change_vector

    @classmethod
    def from_json(cls, json_dict: dict) -> PutResult:
        return cls(json_dict["Id"], json_dict["ChangeVector"])


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
            "DELETE", f"{node.url}/databases/{node.database}/docs?id={Utils.escape(self.__key, None, None)}"
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
        url = f"{node.url}/databases/{node.database}/docs?id={Utils.escape(self.__key, True, False)}"
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


class GetDocumentsCommand(RavenCommand[GetDocumentsResult]):
    def __init__(self):
        """Creates hollow GetDocumentsCommand.
        Class methods ('from_*') instantiate this class by this constructor - then the object is filled with data."""
        super(GetDocumentsCommand, self).__init__(GetDocumentsResult)
        self._keys: Optional[List[str]] = None
        self._includes: Optional[List[str]] = None
        self._metadata_only: Optional[bool] = None

        self._key: Optional[str] = None
        self._counters: Optional[List[str]] = None
        self._include_all_counters: Optional[bool] = None
        self._time_series_includes: Optional[List] = None  # todo: AbstractTimeSeriesRange

        self._compare_exchange_value_includes: Optional[List[str]] = None

        self._start_with: Optional[str] = None
        self._matches: Optional[str] = None
        self._start: Optional[int] = None
        self._page_size: Optional[int] = None
        self._exclude: Optional[str] = None
        self._start_after: Optional[str] = None

    @classmethod
    def from_paging(cls, start: int, page_size: int) -> GetDocumentsCommand:
        command = cls()
        command._start = start
        command._page_size = page_size
        return command

    @classmethod
    def from_single_id(cls, key: str, includes: List[str] = None, metadata_only: bool = None) -> GetDocumentsCommand:
        if key is None:
            raise ValueError("Key cannot be None")
        command = cls()
        command._key = key
        command._includes = includes
        command._metadata_only = metadata_only
        return command

    @classmethod
    def from_multiple_ids(
        cls,
        keys: List[str],
        includes: List[str] = None,
        counter_includes: List[str] = None,
        time_series_includes: List[str] = None,
        compare_exchange_value_includes: List[str] = None,
        metadata_only: bool = False,
    ) -> GetDocumentsCommand:
        if not keys:
            raise ValueError("Please supply at least one id")
        command = cls()
        command._keys = keys
        command._includes = includes
        command._metadata_only = metadata_only

        command._counters = counter_includes
        command._time_series_includes = time_series_includes
        command._compare_exchange_value_includes = compare_exchange_value_includes
        return command

    @classmethod
    def from_multiple_ids_all_counters(
        cls,
        keys: List[str],
        includes: List[str] = None,
        include_all_counters: bool = None,
        time_series_includes: List[str] = None,
        compare_exchange_value_includes: List[str] = None,
        metadata_only: bool = False,
    ) -> GetDocumentsCommand:
        if not keys:
            raise ValueError("Please supply at least one id")
        command = cls()
        command._keys = keys
        command._includes = includes
        command._include_all_counters = include_all_counters
        command._time_series_includes = time_series_includes
        command._compare_exchange_value_includes = compare_exchange_value_includes
        command._metadata_only = metadata_only
        return command

    @classmethod
    def from_starts_with(
        cls,
        start_with: str,
        start_after: str = None,
        matches: str = None,
        exclude: str = None,
        start: int = None,
        page_size: int = None,
        metadata_only: bool = None,
    ) -> GetDocumentsCommand:
        if start_with is None:
            raise ValueError("start_with cannot be None")

        command = cls()
        command._start_with = start_with
        command._start_after = start_after
        command._matches = matches
        command._exclude = exclude
        command._start = start
        command._page_size = page_size
        command._metadata_only = metadata_only

        return command

    def create_request(self, node: ServerNode) -> requests.Request:
        path_builder: List[str] = [node.url, f"/databases/{node.database}/docs?"]

        if self._start is not None:
            path_builder.append(f"&start={self._start}")

        if self._page_size is not None:
            path_builder.append(f"&pageSize={self._page_size}")

        if self._metadata_only:
            path_builder.append("&metadataOnly=true")

        if self._start_with is not None:
            path_builder.append(f"&startsWith={Utils.quote_key(self._start_with)}")

            if self._matches is not None:
                path_builder.append(f"&matches={self._matches}")

            if self._exclude is not None:
                path_builder.append(f"&exclude={self._exclude}")

            if self._start_after is not None:
                path_builder.append(f"&startAfter={self._start_after}")

        if self._includes is not None:
            for include in self._includes:
                path_builder.append(f"&include={include}")

        # todo: counters
        if self._include_all_counters:
            path_builder.append(f"&counter={constants.Counters.ALL}")
        elif self._counters:
            for counter in self._counters:
                path_builder.append(f"&counter={counter}")

        # todo: time series
        if self._time_series_includes is not None:
            for time_series in self._time_series_includes:
                path_builder.append(
                    f"&timeseries={time_series.name}&from={time_series.from_date}&to={time_series.to_date}"
                )

        if self._compare_exchange_value_includes is not None:
            for compare_exchange_value in self._compare_exchange_value_includes:
                path_builder.append(f"&cmpxchg={compare_exchange_value}")

        request = requests.Request("GET")

        if self._key is not None:
            path_builder.append(f"&id={Utils.quote_key(self._key)}")
        if self._keys is not None:
            request = self.prepare_request_with_multiple_ids(path_builder, request, self._keys)

        request.url = "".join(path_builder)
        return request

    def prepare_request_with_multiple_ids(
        self, path_builder: List[str], request: requests.Request, ids: List[str]
    ) -> requests.Request:
        unique_ids = set()
        unique_ids.update(ids)
        is_get = sum(map(len, list(filter(lambda x: x, unique_ids)))) < 1024

        if is_get:
            for key in unique_ids:
                path_builder.append(f"&id={Utils.quote_key(key) if key else ''}")

            return requests.Request("GET")

        else:
            http_post = requests.Request("POST")
            try:
                calculate_hash = HashCalculator.calculate_hash_from_str_collection(unique_ids)
                path_builder.append(f"&loadHash={calculate_hash}")
            except Exception as e:
                raise RuntimeError(f"Unable to computer query hash: {e.args[0]}", e)

            http_post.data = {"Ids": [str(key) for key in unique_ids]}
            return http_post

    def set_response(self, response: str, from_cache: bool) -> None:
        self.result = GetDocumentsResult.from_json(json.loads(response)) if response is not None else None

    @property
    def is_read_request(self) -> bool:
        return True


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
        path = (
            f"/hilo/next?tag={Utils.escape(self.__tag, False, False)}"
            f"&lastBatchSize={self.__last_batch_size or 0}"
            f"&lastRangeAt={date}"
            f"&identityPartsSeparator={self.__identity_parts_separator}"
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

    def create_request(self, node: ServerNode) -> requests.Request:
        return requests.Request(
            "PUT",
            url=f"{node.url}/databases/{node.database}/hilo/return?tag={self.__tag}&end={self.__end}&last={self.__last}",
        )


class HeadAttachmentCommand(RavenCommand[str]):
    def __init__(self, document_id: str, name: str, change_vector: str):
        super().__init__(str)

        if document_id.isspace():
            raise ValueError("Document_id cannot be None or whitespace")

        if name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        self.__document_id = document_id
        self.__name = name
        self.__change_vector = change_vector

    def create_request(self, node: ServerNode) -> requests.Request:
        return requests.Request(
            "HEAD",
            f"{node.url}/databases/{node.database}"
            f"/attachments?id={Utils.quote_key(self.__document_id)}"
            f"&name={Utils.quote_key(self.__name)}",
            {"If-None-Match": self.__change_vector} if self.__change_vector else None,
        )

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> http.ResponseDisposeHandling:
        if response.status_code == http.HTTPStatus.NOT_MODIFIED:
            self.result = self.__change_vector
            return ResponseDisposeHandling.AUTOMATIC

        if response.status_code == http.HTTPStatus.NOT_FOUND:
            self.result = None
            return ResponseDisposeHandling.AUTOMATIC

        chv = response.headers.get(constants.Headers.ETAG, None)
        if not chv:
            raise RuntimeError("Response didn't had an ETag header")
        self.result = chv
        return ResponseDisposeHandling.AUTOMATIC

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is not None:
            self._throw_invalid_response()

        self.result = None

    def is_read_request(self) -> bool:
        return False


class ConditionalGetResult:
    def __init__(self, results: List = None, change_vector: str = None):
        self.results = results
        self.change_vector = change_vector

    @classmethod
    def from_json(cls, json_dict: dict) -> ConditionalGetResult:
        return cls(json_dict["Results"], None)


class ConditionalGetDocumentsCommand(RavenCommand[ConditionalGetResult]):
    def __init__(self, key: str, change_vector: str):
        super().__init__(ConditionalGetResult)

        self.__change_vector = change_vector
        self.__key = key

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/docs?id={Utils.quote_key(self.__key)}"
        request = requests.Request("GET", url)
        request.headers["If-None-Match"] = f'"{self.__change_vector}"'

        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = None
            return

        self.result = ConditionalGetResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return False

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
        if response.status_code == http.HTTPStatus.NOT_MODIFIED:
            return ResponseDisposeHandling.AUTOMATIC

        result = super().process_response(cache, response, url)
        self.result.change_vector = response.headers["ETag"]
        return result
