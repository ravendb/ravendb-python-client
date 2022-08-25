import datetime
import json
from typing import TYPE_CHECKING, Union, Optional

import requests

from ravendb.documents.operations.definitions import MaintenanceOperation, OperationIdResult, IOperation
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents import DocumentStore


class GetOperationStateOperation(MaintenanceOperation):
    def __init__(self, key: int, node_tag: str = None):
        self.__key = key
        self.__node_tag = node_tag

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand:
        return self.GetOperationStateCommand(self.__key, self.__node_tag)

    class GetOperationStateCommand(RavenCommand[dict]):
        def __init__(self, key: int, node_tag: str = None):
            super().__init__(dict)
            self.__key = key
            self.__selected_node_tag = node_tag

            self.__timeout = datetime.timedelta(seconds=15)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{node.url}/databases/{node.database}/operations/state?id={self.__key}")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = json.loads(response)


class QueryOperationOptions(object):
    def __init__(
        self,
        allow_stale: bool = True,
        stale_timeout: datetime.timedelta = None,
        max_ops_per_sec: int = None,
        retrieve_details: bool = False,
    ):
        self.allow_stale = allow_stale
        self.stale_timeout = stale_timeout
        self.retrieve_details = retrieve_details
        self.max_ops_per_sec = max_ops_per_sec


class DeleteByQueryOperation(IOperation[OperationIdResult]):
    def __init__(self, query_to_delete: Union[str, IndexQuery], options: Optional[QueryOperationOptions] = None):
        if not query_to_delete:
            raise ValueError("Invalid query")
        if isinstance(query_to_delete, str):
            query_to_delete = IndexQuery(query=query_to_delete)
        self.__query_to_delete = query_to_delete
        self.__options = options if options is not None else QueryOperationOptions()

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: Optional[HttpCache] = None
    ) -> RavenCommand[OperationIdResult]:
        return self.__DeleteByIndexCommand(conventions, self.__query_to_delete, self.__options)

    class __DeleteByIndexCommand(RavenCommand[OperationIdResult]):
        def __init__(
            self,
            conventions: "DocumentConventions",
            query_to_delete: IndexQuery,
            options: Optional[QueryOperationOptions] = None,
        ):
            super().__init__(OperationIdResult)
            self.__conventions = conventions
            self.__query_to_delete = query_to_delete
            self.__options = options if options is not None else QueryOperationOptions()

        def create_request(self, server_node):
            url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = (
                f"?allowStale={self.__options.allow_stale}&maxOpsPerSec={self.__options.max_ops_per_sec or ''}"
                f"&details={self.__options.retrieve_details}"
            )
            if self.__options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self.__options.stale_timeout)

            url += path
            data = self.__query_to_delete.to_json()
            return requests.Request("DELETE", url, data=data)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            response = json.loads(response)
            self.result = OperationIdResult(response["OperationId"], response["OperationNodeTag"])

        def is_read_request(self) -> bool:
            return False
