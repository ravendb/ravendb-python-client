from __future__ import annotations

import datetime
import enum
import json
from abc import abstractmethod
from typing import Generic, TypeVar, TYPE_CHECKING, Optional, List
import requests

from pyravendb import constants
from pyravendb.documents.operations.operation import Operation
from pyravendb.tools.utils import Utils
from pyravendb.http.raven_command import RavenCommand
from pyravendb.util.util import RaftIdGenerator
from pyravendb.http import RaftCommand
import pyravendb.serverwide

if TYPE_CHECKING:
    from pyravendb.http import VoidRavenCommand, ServerNode
    from pyravendb.http.request_executor import RequestExecutor
    from pyravendb.data.document_conventions import DocumentConventions
    from pyravendb.serverwide import DatabaseRecord

T = TypeVar("T")


class DatabasePromotionStatus(enum.Enum):
    WAITING_FOR_FIRST_PROMOTION = "WAITING_FOR_FIRST_PROMOTION"
    NOT_RESPONDING = "NOT_RESPONDING"
    INDEX_NOT_UP_TO_DATE = "INDEX_NOT_UP_TO_DATE"
    CHANGE_VECTOR_NOT_MERGED = "CHANGE_VECTOR_NOT_MERGED"
    WAITING_FOR_RESPONSE = "WAITING_FOR_RESPONSE"
    OK = "OK"
    OUT_OF_CPU_CREDITS = "OUT_OF_CPU_CREDITS"
    EARLY_OUT_OF_MEMORY = "EARLY_OUT_OF_MEMORY"
    HIGH_DIRTY_MEMORY = "HIGH_DIRTY_MEMORY"

    def __str__(self):
        return self.value


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


class DatabasePutResult:
    def __init__(
        self,
        raft_command_index: Optional[int] = None,
        name: Optional[str] = None,
        topology: Optional[pyravendb.serverwide.DatabaseTopology] = None,
        nodes_added_to: Optional[List[str]] = None,
    ):
        self.raft_command_index = raft_command_index
        self.name = name
        self.topology = topology
        self.nodes_added_to = nodes_added_to

    @staticmethod
    def from_json(json_dict: dict) -> DatabasePutResult:
        return DatabasePutResult(
            json_dict["RaftCommandIndex"],
            json_dict["Name"],
            pyravendb.serverwide.DatabaseTopology.from_json(json_dict["Topology"]),
            json_dict["NodesAddedTo"],
        )


class CreateDatabaseOperation(ServerOperation):
    def __init__(self, database_record: DatabaseRecord, replication_factor: Optional[int] = 1):
        self.__database_record = database_record
        self.__replication_factor = replication_factor

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return CreateDatabaseOperation.CreateDatabaseCommand(
            conventions, self.__database_record, self.__replication_factor
        )

    class CreateDatabaseCommand(RavenCommand[DatabasePutResult], RaftCommand):
        def __init__(
            self,
            conventions: "DocumentConventions",
            database_record: DatabaseRecord,
            replication_factor: int,
            etag: Optional[int] = None,
        ):
            super().__init__(DatabasePutResult)
            self.__conventions = conventions
            self.__database_record = database_record
            self.__replication_factor = replication_factor
            self.__etag = etag
            self.__database_name = database_record.database_name
            if self.__database_name is None:
                raise ValueError("Database name is required.")

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/admin/databases?name={self.__database_name}&replicationFactor={self.__replication_factor}"
            )

            request = requests.Request("PUT")
            request.data = self.__database_record.to_json()
            request.headers["Content-type"] = "application/json"
            request.url = url

            if self.__etag is not None:
                request.headers[constants.Headers.ETAG] = f'"{self.__etag}"'

            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = DatabasePutResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False

        @property
        def raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DeleteDatabaseResult:
    def __init__(self, raft_command_index: Optional[int] = None, pending_deletes: Optional[List[str]] = None):
        self.raft_command_index = raft_command_index
        self.pending_deletes = pending_deletes

    @staticmethod
    def from_json(json_dict) -> DeleteDatabaseResult:
        return DeleteDatabaseResult(json_dict["RaftCommandIndex"], json_dict["PendingDeletes"])


class DeleteDatabaseOperation(ServerOperation[DeleteDatabaseResult]):
    class Parameters:
        def __init__(
            self,
            database_names: Optional[List[str]] = None,
            hard_delete: Optional[bool] = None,
            from_nodes: Optional[List[str]] = None,
            time_to_wait_for_confirmation: Optional[datetime.timedelta] = None,
        ):
            self.database_names = database_names
            self.hard_delete = hard_delete
            self.from_nodes = from_nodes
            self.time_to_wait_for_confirmation = time_to_wait_for_confirmation

        def to_json(self) -> dict:
            return {
                "DatabaseNames": self.database_names,
                "HardDelete": self.hard_delete,
                "FromNodes": self.from_nodes,
                "TimeToWaitForConfirmation": Utils.timedelta_to_str(self.time_to_wait_for_confirmation),
            }

    def __init__(
        self,
        database_name: Optional[str] = None,
        hard_delete: Optional[bool] = None,
        from_node: Optional[str] = None,
        time_to_wait_for_confirmation: Optional[datetime.timedelta] = None,
        parameters: Optional[DeleteDatabaseOperation.Parameters] = None,
    ):
        if not ((parameters is not None) ^ (database_name is not None)):
            raise ValueError(
                "Please specify either parameters (Parameters object) or database_name (and optionally other args)."
            )
        self.__parameters = (
            parameters
            if parameters
            else DeleteDatabaseOperation.Parameters(
                [database_name], hard_delete, [from_node] if from_node else None, time_to_wait_for_confirmation
            )
        )

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.__DeleteDatabaseCommand(conventions, self.__parameters)

    class __DeleteDatabaseCommand(RavenCommand[DeleteDatabaseResult], RaftCommand):
        def __init__(self, conventions: DocumentConventions, parameters: DeleteDatabaseOperation.Parameters):
            super().__init__(DeleteDatabaseResult)

            if conventions is None:
                raise ValueError("Conventions cannot be None")

            if parameters is None:
                raise ValueError("Parameters cannot be None")

            self.__parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request(
                "DELETE", f"{node.url}/admin/databases?raft-request-id={self.raft_unique_request_id()}"
            )
            request.headers["Content-type"] = "application/json"
            request.data = self.__parameters.to_json()
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            DeleteDatabaseResult.from_json(json.loads(response))


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