import json
from typing import TYPE_CHECKING

import requests

from ravendb.documents.queries.query import QueryResult
from ravendb.extensions.json_extensions import JsonExtensions
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    from ravendb.documents.session.document_session import InMemoryDocumentSessionOperations
    from ravendb.documents.queries.index_query import IndexQuery


class QueryCommand(RavenCommand[QueryResult]):
    def __init__(
        self,
        session: "InMemoryDocumentSessionOperations",
        index_query: "IndexQuery",
        metadata_only: bool,
        index_entries_only: bool,
    ):
        super().__init__(QueryResult)
        self.__session = session
        if index_query is None:
            raise ValueError("index_query cannot be None")

        self.__index_query = index_query
        self.__metadata_only = metadata_only
        self.__index_entries_only = index_entries_only

    def create_request(self, node: ServerNode) -> requests.Request:
        self._can_cache = not self.__index_query.disable_caching
        self._can_cache_aggressively = self._can_cache and not self.__index_query.wait_for_non_stale_results

        path = [node.url, "/databases/", node.database, "/queries?queryHash=", self.__index_query.get_query_hash()]

        if self.__metadata_only:
            path.append("&metadataOnly=true")

        if self.__index_entries_only:
            path.append("&debug=entries")

        request = requests.Request("POST", "".join(path))
        request.data = JsonExtensions.write_index_query(self.__session.conventions, self.__index_query)
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self.result = None
            return

        self.result = QueryResult.from_json(json.loads(response))
        if from_cache:
            self.result.duration_in_ms = -1

            if self.result.timings is not None:
                self.result.timings.duration_in_ms = -1
                self.result.timings = None

    def is_read_request(self) -> bool:
        return True
