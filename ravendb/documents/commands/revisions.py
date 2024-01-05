from __future__ import annotations

import json
from datetime import datetime
from typing import List, Optional, TYPE_CHECKING

import requests

from ravendb.http.raven_command import RavenCommand
from ravendb.json.result import JsonArrayResult
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.http.server_node import ServerNode


class GetRevisionsCommand(RavenCommand[JsonArrayResult]):
    def __init__(
        self,
        id_: str = None,
        start: int = None,
        page_size: int = None,
        metadata_only: bool = None,
        before: datetime = None,
        change_vector: str = None,
        change_vectors: List[str] = None,
    ):
        super().__init__(JsonArrayResult)
        self._id = id_
        self._start = start
        self._page_size = page_size
        self._metadata_only = metadata_only
        self._before = before
        self._change_vector = change_vector
        self._change_vectors = change_vectors

    @property
    def change_vectors(self) -> Optional[List[str]]:
        return self._change_vectors

    @property
    def change_vector(self) -> Optional[str]:
        return self._change_vector

    @property
    def before(self) -> Optional[datetime]:
        return self._before

    @property
    def id(self) -> Optional[str]:
        return self._id

    @classmethod
    def from_change_vector(cls, change_vector: str, metadata_only: bool = False) -> GetRevisionsCommand:
        return GetRevisionsCommand(change_vector=change_vector, metadata_only=metadata_only)

    @classmethod
    def from_change_vectors(cls, change_vectors: List[str] = None, metadata_only: bool = False) -> GetRevisionsCommand:
        return GetRevisionsCommand(change_vectors=change_vectors, metadata_only=metadata_only)

    @classmethod
    def from_before(cls, id_: str, before: datetime) -> GetRevisionsCommand:
        if id_ is None:
            raise ValueError("Id cannot be None")
        return GetRevisionsCommand(id_=id_, before=before)

    @classmethod
    def from_start_page(
        cls, id_: str = None, start: int = None, page_size: int = None, metadata_only: bool = False
    ) -> GetRevisionsCommand:
        if id_ is None:
            raise ValueError("Id cannot be None")

        return cls(id_=id_, start=start, page_size=page_size, metadata_only=metadata_only)

    def create_request(self, node: ServerNode) -> requests.Request:
        path_builder = [node.url, "/databases/", node.database, "/revisions?"]

        self.get_request_query_string(path_builder)
        return requests.Request("GET", "".join(path_builder))

    def get_request_query_string(self, path_builder: List[str]) -> None:
        if self._id is not None:
            path_builder.append("&id=")
            path_builder.append(Utils.escape(self._id))
        elif self._change_vector is not None:
            path_builder.append("&changeVector=")
            path_builder.append(Utils.escape(self._change_vector))
        elif self._change_vectors is not None:
            for change_vector in self._change_vectors:
                path_builder.append("&changeVector=")
                path_builder.append(Utils.escape(change_vector))

        if self._before is not None:
            path_builder.append("&before")
            path_builder.append(Utils.datetime_to_string(self._before))

        if self._start is not None:
            path_builder.append("&start=")
            path_builder.append(str(self._start))

        if self._page_size is not None:
            path_builder.append("&pageSize=")
            path_builder.append(str(self._page_size))

        if self._metadata_only:
            path_builder.append("&metadataOnly=true")

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self.result = None
            return

        self.result = JsonArrayResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return True


class GetRevisionsBinEntryCommand(RavenCommand[JsonArrayResult]):
    def __init__(self, etag: int, page_size: int):
        super().__init__(JsonArrayResult)
        self._etag = etag
        self._page_size = page_size

    def create_request(self, node: ServerNode) -> requests.Request:
        request = requests.Request("GET")
        path = [node.url, "/databases/", node.database, "/revisions/bin?start=", str(self._etag)]
        if self._page_size is not None:
            path.append("&pageSize=")
            path.append(str(self._page_size))

        request.url = "".join(path)

        return request

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self._throw_invalid_response()

        self.result = JsonArrayResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return True
