import json
from abc import abstractmethod
from typing import Generic, TypeVar, TYPE_CHECKING
import requests

from pyravendb.tools.utils import Utils
from pyravendb.documents.operations import Operation
from pyravendb.http.raven_command import RavenCommand

if TYPE_CHECKING:
    from pyravendb.http import VoidRavenCommand, ServerNode
    from pyravendb.http.request_executor import RequestExecutor
    from pyravendb.data.document_conventions import DocumentConventions

T = TypeVar("T")


class ServerOperation(Generic[T]):
    @abstractmethod
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        raise NotImplementedError()


class VoidServerOperation(ServerOperation[None]):
    @abstractmethod
    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        raise NotImplementedError()


class ServerWideOperation(Operation):
    def __init__(
        self, request_executor: "RequestExecutor", conventions: "DocumentConventions", key: int, node_tag: str = None
    ):
        super(ServerWideOperation, self).__init__(request_executor, None, conventions, key, node_tag)
        self.node_tag = node_tag

    def _get_operation_state_command(
        self, conventions: "DocumentConventions", key: int, node_tag: str = None
    ) -> RavenCommand[dict]:
        return GetServerWideOperationStateOperation.GetServerWideOperationStateCommand(key, node_tag)


class GetServerWideOperationStateOperation(ServerOperation[dict]):
    def __init__(self, key: int):
        self.__key = key

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.GetServerWideOperationStateCommand(self.__key)

    class GetServerWideOperationStateCommand(RavenCommand[dict]):
        def __init__(self, key: int, node_tag: str = None):
            super().__init__(dict)

            self.__key = key
            self._selected_node_tag = node_tag

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: "ServerNode") -> requests.Request:
            return requests.Request("GET", f"{node.url}/operations/state?id={self.__key}")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = json.loads(response)


class BuildNumber:
    def __init__(
        self, product_version: str = None, build_version: int = None, commit_hash: str = None, full_version: str = None
    ):
        self.product_version = product_version
        self.build_version = build_version
        self.commit_hash = commit_hash
        self.full_version = full_version


class GetBuildNumberOperation(ServerOperation[BuildNumber]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.GetBuildNumberCommand()

    class GetBuildNumberCommand(RavenCommand[BuildNumber]):
        def __init__(self):
            super().__init__(BuildNumber)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: "ServerNode") -> requests.Request:
            return requests.Request("GET", f"{node.url}/build/version")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = Utils.initialize_object(response, self._result_class, True)
