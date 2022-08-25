from __future__ import annotations
import enum
import json
from typing import List, TYPE_CHECKING

import requests

from ravendb.exceptions import exceptions
from ravendb.exceptions.exceptions import ErrorResponseException
from ravendb.documents.indexes.definitions import IndexDefinition, IndexErrors, IndexLockMode, IndexPriority
from ravendb.documents.operations.definitions import MaintenanceOperation, VoidMaintenanceOperation
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class PutIndexesOperation(MaintenanceOperation):
    def __init__(self, *indexes_to_add):
        if len(indexes_to_add) == 0:
            raise ValueError("Invalid indexes_to_add")

        super(PutIndexesOperation, self).__init__()
        self._indexes_to_add = indexes_to_add
        self.__all_java_script_indexes = True  # todo: set it in the command

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[List]:
        return self.__PutIndexesCommand(conventions, *self._indexes_to_add)

    class __PutIndexesCommand(RavenCommand[List], RaftCommand):
        def __init__(self, conventions: "DocumentConventions", *index_to_add):
            super().__init__(list)
            if conventions is None:
                raise ValueError("Conventions cannot be None")
            if index_to_add is None:
                raise ValueError("None indexes_to_add is not valid")

            self.indexes_to_add = []
            for index_definition in index_to_add:
                if not isinstance(index_definition, IndexDefinition):
                    raise ValueError("index_definition in indexes_to_add must be IndexDefinition type")
                if index_definition.name is None:
                    raise ValueError("None Index name is not valid")
                self.indexes_to_add.append(index_definition.to_json())

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request("PUT")
            request.url = f"{node.url}/databases/{node.database}/admin/indexes"
            request.data = {"Indexes": self.indexes_to_add}
            request.headers["Content-type"] = "application/json"
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = json.loads(response)  # todo: PutIndexResult instead of dict
            if "Error" in response:
                raise ErrorResponseException(response["Error"])


class GetIndexNamesOperation(MaintenanceOperation):
    def __init__(self, start, page_size):
        super().__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions) -> RavenCommand[List[str]]:
        return self.__GetIndexNamesCommand(self._start, self._page_size)

    class __GetIndexNamesCommand(RavenCommand[List[str]]):
        def __init__(self, start, page_size):
            super().__init__(list)
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            return requests.Request(
                "GET",
                f"{server_node.url}/databases/{server_node.database}/indexes?start={self._start}&pageSize={self._page_size}&namesOnly=true",
            )

        def is_read_request(self) -> bool:
            return True

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                raise ValueError("Invalid response")
            response = json.loads(response)
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")
            self.result = response["Results"]


class DeleteIndexOperation(VoidMaintenanceOperation):
    def __init__(self, index_name: str):
        if index_name is None:
            raise ValueError("Index name cannot be None")
        super(DeleteIndexOperation, self).__init__()
        self.__index_name = index_name

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__DeleteIndexCommand(self.__index_name)

    class __DeleteIndexCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, index_name: str):
            if not index_name:
                raise ValueError("Invalid index_name")
            super().__init__()
            self.__index_name = index_name

        def create_request(self, server_node: ServerNode) -> requests.Request:
            return requests.Request(
                "DELETE",
                f"{server_node.url}/databases/{server_node.database}/indexes?name={Utils.quote_key(self.__index_name)}",
            )

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DisableIndexOperation(VoidMaintenanceOperation):
    def __init__(self, index_name: str, cluster_wide: bool = False):
        if index_name is None:
            raise ValueError("index_name cannot be None")
        super().__init__()
        self.__index_name = index_name
        self.__cluster_wide = cluster_wide

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__DisableIndexCommand(self.__index_name, self.__cluster_wide)

    class __DisableIndexCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, index_name: str, cluster_wide: bool):
            if index_name is None:
                raise ValueError("index_name cannot be None")
            super().__init__()
            self.__index_name = index_name
            self.__cluster_wide = cluster_wide

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "POST",
                f"{server_node.url}/databases/{server_node.database}/admin/indexes"
                f"/disable?name={Utils.escape(self.__index_name, False, False)}"
                f"&clusterWide={self.__cluster_wide}",
            )

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class EnableIndexOperation(VoidMaintenanceOperation):
    def __init__(self, index_name: str, cluster_wide: bool = False):
        if index_name is None:
            raise ValueError("index_name cannot be None")
        super().__init__()
        self.__index_name = index_name
        self.__cluster_wide = cluster_wide

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__EnableIndexCommand(self.__index_name, self.__cluster_wide)

    class __EnableIndexCommand(VoidRavenCommand):
        def __init__(self, index_name: str, cluster_wide: bool):
            if index_name is None:
                raise ValueError("index_name cannot be None")
            super().__init__()
            self.__index_name = index_name
            self.__cluster_wide = cluster_wide

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "POST",
                f"{server_node.url}/databases/{server_node.database}/admin/indexes"
                f"/enable?name={Utils.escape(self.__index_name, False, False)}"
                f"&clusterWide={self.__cluster_wide}",
            )

        def raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class StopIndexingOperation(VoidMaintenanceOperation):
    def __init__(self):
        super().__init__()

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__StopIndexingCommand()

    class __StopIndexingCommand(VoidRavenCommand):
        def __init__(self):
            super().__init__()

        def create_request(self, server_node) -> requests.Request:
            return requests.Request("POST", f"{server_node.url}/databases/{server_node.database}/admin/indexes/stop")


class StartIndexingOperation(VoidMaintenanceOperation):
    def __init__(self):
        super().__init__()

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__StopIndexingCommand()

    class __StopIndexingCommand(VoidRavenCommand):
        def __init__(self):
            super().__init__()

        def create_request(self, server_node) -> requests.Request:
            return requests.Request("POST", f"{server_node.url}/databases/{server_node.database}/admin/indexes/start")


class StartIndexOperation(VoidMaintenanceOperation):
    def __init__(self, index_name: str):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        super().__init__()
        self.__index_name = index_name

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__StartIndexCommand(self.__index_name)

    class __StartIndexCommand(VoidRavenCommand):
        def __init__(self, index_name):
            super().__init__()
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "POST",
                f"{server_node.url}/databases/{server_node.database}"
                f"/admin/indexes/start?name={Utils.quote_key(self._index_name)}",
            )


class StopIndexOperation(VoidMaintenanceOperation):
    def __init__(self, index_name: str):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        super().__init__()
        self.__index_name = index_name

    def get_command(self, conventions) -> VoidRavenCommand:
        return self.__StopIndexCommand(self.__index_name)

    class __StopIndexCommand(VoidRavenCommand):
        def __init__(self, index_name):
            super().__init__()
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "POST",
                f"{server_node.url}/databases/{server_node.database}"
                f"/admin/indexes/stop?name={Utils.quote_key(self._index_name)}",
            )


class IndexRunningStatus(enum.Enum):
    RUNNING = "Running"
    PAUSED = "Paused"
    DISABLED = "Disabled"
    PENDING = "Pending"


class IndexStatus:
    def __init__(self, name: str = None, status: IndexRunningStatus = None):
        self.name = name
        self.status = status

    @classmethod
    def from_json(cls, json_dict: dict) -> IndexStatus:
        return cls(json_dict["Name"], IndexRunningStatus(json_dict["Status"]))


class IndexingStatus:
    def __init__(
        self, status: IndexRunningStatus = None, indexes: List[IndexStatus] = None
    ):  # todo: Optional typehints
        self.status = status
        self.indexes = indexes

    @classmethod
    def from_json(cls, json_dict) -> IndexingStatus:
        return cls(IndexRunningStatus(json_dict["Status"]), list(map(IndexStatus.from_json, json_dict["Indexes"])))


class GetIndexingStatusOperation(MaintenanceOperation[IndexingStatus]):
    def __init__(self):
        super(GetIndexingStatusOperation, self).__init__()

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[IndexingStatus]:
        return self.__GetIndexingStatusCommand()

    class __GetIndexingStatusCommand(RavenCommand[IndexingStatus]):
        def __init__(self):
            super().__init__(IndexingStatus)

        def create_request(self, server_node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{server_node.url}/databases/{server_node.database}/indexes/status")

        def is_read_request(self) -> bool:
            pass

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                raise ValueError("Invalid response")
            response = json.loads(response)
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            self.result = IndexingStatus.from_json(response)


class GetIndexStatisticsOperation(MaintenanceOperation[dict]):  # replace returned type with IndexStats -> its enormous
    def __init__(self, name):
        if name is None:
            raise ValueError("Index name cannot be None")
        super().__init__()
        self.__index_name = name

    def get_command(self, conventions) -> RavenCommand[dict]:
        return self.__GetIndexStatisticsCommand(self.__index_name)

    class __GetIndexStatisticsCommand(RavenCommand[dict]):
        def __init__(self, index_name: str):
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            super().__init__(dict)
            self.__index_name = index_name

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "GET",
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes/stats?name={Utils.escape(self.__index_name, False, False)}",
            )

        def is_read_request(self) -> bool:
            return True

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                raise ValueError("Invalid response")

            response = json.loads(response)
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response or len(response["Results"]) > 1:
                raise ValueError("Invalid response")
            self.result = response["Results"]


class GetIndexesStatisticsOperation(
    MaintenanceOperation[dict]
):  # replace returned type with IndexStats -> its enormous
    def get_command(self, conventions) -> RavenCommand[dict]:
        return self.__GetIndexesStatisticsCommand()

    class __GetIndexesStatisticsCommand(RavenCommand[dict]):
        def __init__(self):
            super().__init__(dict)

        def create_request(self, server_node) -> requests.Request:
            return requests.Request("GET", f"{server_node.url}/databases/{server_node.database}/indexes/stats")

        def is_read_request(self) -> bool:
            return True

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                raise ValueError("Invalid response")

            response = json.loads(response)
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            self.result = response["Results"]


class GetIndexErrorsOperation(MaintenanceOperation[List[IndexErrors]]):
    def __init__(self, *index_names: str):
        super(GetIndexErrorsOperation, self).__init__()
        self._index_names = index_names

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[List[IndexErrors]]:
        return self.__GetIndexErrorsCommand(*self._index_names)

    class __GetIndexErrorsCommand(RavenCommand[List[IndexErrors]]):
        def __init__(self, *index_names: str):
            super().__init__(list)
            self._index_names = index_names

        def create_request(self, server_node) -> requests.Request:
            url = f"{server_node.url}/databases/{server_node.database}/indexes/errors"
            if self._index_names and len(self._index_names) != 0:
                url += "?"
                for index_name in self._index_names:
                    url += f"&name={Utils.escape(index_name, None, None)}"
            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool):
            if response is None:
                raise ValueError("InvalidResponse")
            results = json.loads(response)["Results"]
            self.result = []
            for result in results:
                self.result.append(IndexErrors.from_json(result))

        def is_read_request(self) -> bool:
            return True


class GetIndexesOperation(MaintenanceOperation[List[IndexDefinition]]):
    def __init__(self, start: int, page_size: int):
        super(GetIndexesOperation, self).__init__()
        self.__start = start
        self.__page_size = page_size

    def get_command(self, conventions):
        return self.__GetIndexesCommand(self.__start, self.__page_size)

    class __GetIndexesCommand(RavenCommand[List[IndexDefinition]]):
        def __init__(self, start: int, page_size: int):
            super().__init__(list)
            self.__start = start
            self.__page_size = page_size

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "GET",
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes?start={self.__start}&pageSize{self.__page_size} ",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            response = json.loads(response)["Results"]
            result = []
            for index in response:
                result.append(IndexDefinition.from_json(index))
            self.result = result

        def is_read_request(self) -> bool:
            return True


class GetIndexOperation(MaintenanceOperation[IndexDefinition]):
    def __init__(self, index_name: str):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        super().__init__()
        self.__index_name = index_name

    def get_command(self, conventions: "DocumentConventions"):
        return self.__GetIndexCommand(self.__index_name)

    class __GetIndexCommand(RavenCommand[IndexDefinition]):
        def __init__(self, index_name: str):
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            super().__init__(IndexDefinition)
            self.__index_name = index_name

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "GET",
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes?name={Utils.escape(self.__index_name,False,False)}",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            if not response:
                return
            self.result = IndexDefinition.from_json(json.loads(response)["Results"][0])

        def is_read_request(self) -> bool:
            return True


class SetIndexesLockOperation(VoidMaintenanceOperation):
    def __init__(self, mode: IndexLockMode, *index_names: str):
        if len(index_names) == 0:
            raise ValueError("Index names count cannot be zero. Pass at least one index name.")
        super(SetIndexesLockOperation, self).__init__()
        self.__mode = mode
        self.__index_names = index_names
        self.__filter_auto_indexes()

    def get_command(self, conventions: "DocumentConventions") -> VoidRavenCommand:
        return self.__SetIndexesLockCommand(self.__mode, *self.__index_names)

    def __filter_auto_indexes(self):
        for name in self.__index_names:
            if name.startswith("auto/"):
                raise ValueError("Index list contains Auto-Indexes. Lock Mode is not set for Auto-Indexes")

    class __SetIndexesLockCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, mode: IndexLockMode, *index_names: str):
            super().__init__()
            self._mode = mode
            self._index_names = index_names

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

        def create_request(self, server_node):
            request = requests.Request("POST")
            request.url = f"{server_node.url}/databases/{server_node.database}/indexes/set-lock"
            request.data = {"IndexNames": self._index_names, "Mode": self._mode}
            return request


class SetIndexesPriorityOperation(VoidMaintenanceOperation):
    def __init__(self, priority: IndexPriority, *index_names: str):
        if len(index_names) == 0:
            raise ValueError("Index names count cannot be zero. Pass at least one index name.")
        super(SetIndexesPriorityOperation, self).__init__()
        self._priority = priority
        self._index_names = index_names

    def get_command(self, conventions):
        return self.__SetIndexesPriorityCommand(self._priority, *self._index_names)

    class __SetIndexesPriorityCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, priority: IndexPriority, *index_names: str):
            super().__init__()
            self._priority = priority
            self._index_names = index_names

        def create_request(self, server_node):
            request = requests.Request("POST")
            request.url = f"{server_node.url}/databases/{server_node.database}/indexes/set-priority"
            request.data = {"IndexNames": self._index_names, "Priority": self._priority}
            return request

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetTermsOperation(MaintenanceOperation[List[str]]):
    def __init__(self, index_name: str, field: str, from_value: str, page_size: int = None):
        if index_name is None:
            raise ValueError("Index name cannot be None")
        if field is None:
            raise ValueError("Field cannot be null")
        super(GetTermsOperation, self).__init__()
        self.__index_name = index_name
        self.__field = field
        self.__from_value = from_value
        self.__page_size = page_size

    def get_command(self, conventions):
        return self.__GetTermsCommand(self.__index_name, self.__field, self.__from_value, self.__page_size)

    class __GetTermsCommand(RavenCommand[List[str]]):
        def __init__(self, index_name: str, field: str, from_value: str, page_size: int):
            super().__init__(list)
            self.__index_name = index_name
            self.__field = field
            self.__from_value = from_value
            self.__page_size = page_size

        def create_request(self, server_node) -> requests.Request:
            return requests.Request(
                "GET",
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes/terms?name={Utils.escape(self.__index_name, False, False)}"
                f"&field={Utils.escape(self.__field, False, False)}"
                f"&fromValue={self.__from_value if self.__from_value is not None else ''}"
                f"&pageSize={self.__page_size if self.__page_size is not None else ''}",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                raise ValueError("Invalid response")
            response = json.loads(response)
            self.result = response["Terms"]


class IndexHasChangedOperation(MaintenanceOperation[bool]):
    def __init__(self, index: IndexDefinition):
        if index is None:
            raise ValueError("Index cannot be null")
        super(IndexHasChangedOperation, self).__init__()
        self.__index = index

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[bool]:
        return self.__IndexHasChangedCommand(self.__index)

    class __IndexHasChangedCommand(RavenCommand[bool]):
        def __init__(self, index: IndexDefinition):
            super().__init__(bool)
            self.__index = index

        def is_read_request(self) -> bool:
            return False

        def create_request(self, server_node: ServerNode) -> requests.Request:
            request = requests.Request("POST")
            request.url = f"{server_node.url}/databases/{server_node.database}/indexes/has-changed"
            request.data = {
                "Configuration": self.__index.configuration,
                "Fields": self.__index.fields,
                "LockMode": self.__index.lock_mode,
                "Maps": self.__index.maps,
                "Name": self.__index.name,
                "OutputReduceToCollection": self.__index.output_reduce_to_collection,
                "Priority": self.__index.priority,
                "Reduce": self.__index.reduce,
                "SourceType": self.__index.source_type,
                "Type": self.__index.type,
            }
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if not response:
                raise ValueError("Response is invalid")

            self.result = json.loads(response)["Changed"]
