from typing import Tuple, Set, Union, List, Optional
from pyravendb.commands.raven_commands import RavenCommand, GetStatisticsCommand
from pyravendb.tools.utils import Utils
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexDefinition, IndexErrors, IndexingError, IndexLockMode, IndexPriority
from abc import abstractmethod


class MaintenanceOperation(object):
    __slots__ = ["__operation"]

    def __init__(self):
        self.__operation = "MaintenanceOperation"

    @property
    def operation(self):
        return self.__operation

    @abstractmethod
    def get_command(self, conventions):
        raise NotImplementedError


class PullReplicationDefinition:
    def __init__(self, name, certificates, mentor_node=None, delayed_replication_for=None):
        self.DelayedReplicationFor = delayed_replication_for
        self.Name = name
        self.MentorNode = mentor_node
        self.Certificates = certificates


class ExternalReplication:
    def __init__(
        self,
        name,
        connection_string_name,
        mentor_node=None,
        delayed_replication_for=None,
    ):
        self.DelayedReplicationFor = delayed_replication_for
        self.Name = name
        self.ConnectionStringName = connection_string_name
        self.MentorNode = mentor_node


class PullReplicationAsSink:
    def __init__(
        self,
        hub,
        connection_string_name,
        certificate_base64,
        certificate_password,
        mentor_node=None,
        delayed_replication_for=None,
    ):
        self.DelayedReplicationFor = delayed_replication_for
        self.HubDefinitionName = hub
        self.ConnectionStringName = connection_string_name
        self.MentorNode = mentor_node


class ConnectionString:
    @staticmethod
    def raven(name, database, urls):
        return {
            "Type": "Raven",
            "Name": name,
            "Database": database,
            "TopologyDiscoveryUrls": urls,
        }

    @staticmethod
    def sql(name, factory, connection_string):
        return {
            "Type": "Raven",
            "FactoryName": factory,
            "ConnectionString": connection_string,
        }


class UpdatePullReplicationAsSinkOperation(MaintenanceOperation):
    def __init__(self, definition):

        if definition is None:
            raise ValueError("definition cannot be None")

        super(UpdatePullReplicationAsSinkOperation, self).__init__()
        self._definition = definition

    def get_command(self, conventions):
        return self._UpdatePullReplicationAsSinkCommand(self._definition)

    class _UpdatePullReplicationAsSinkCommand(RavenCommand):
        def __init__(self, definition):
            if definition is None:
                raise ValueError("definition cannot be None")

            super(
                UpdatePullReplicationAsSinkOperation._UpdatePullReplicationAsSinkCommand,
                self,
            ).__init__(method="POST", is_raft_request=True)
            self._definition = definition

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/sink-pull-replication".format(
                server_node.url, server_node.database
            )
            self.data = {"PullReplicationAsSink": self._definition}

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.InvalidOperationException(response["Message"], response["Type"], response["Error"])
            except ValueError:
                raise response.raise_for_status()
            return {"raft_command_index": response["RaftCommandIndex"]}


class PutPullReplicationAsHubOperation(MaintenanceOperation):
    def __init__(self, definition):

        if definition is None:
            raise ValueError("definition cannot be None")

        super(PutPullReplicationAsHubOperation, self).__init__()
        self._definition = definition

    def get_command(self, conventions):
        return self._PutPullReplicationAsHubCommand(self._definition)

    class _PutPullReplicationAsHubCommand(RavenCommand):
        def __init__(self, definition):
            if definition is None:
                raise ValueError("definition cannot be None")

            super(PutPullReplicationAsHubOperation._PutPullReplicationAsHubCommand, self).__init__(
                method="PUT", is_raft_request=True
            )
            self._definition = definition

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/pull-replication/hub".format(
                server_node.url, server_node.database
            )
            self.data = self._definition

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.InvalidOperationException(response["Message"], response["Type"], response["Error"])
            except ValueError:
                raise response.raise_for_status()
            return {"raft_command_index": response["RaftCommandIndex"]}


class UpdateExternalReplicationOperation(MaintenanceOperation):
    def __init__(self, watcher):

        if watcher is None:
            raise ValueError("watcher cannot be None")

        super(UpdateExternalReplicationOperation, self).__init__()
        self._watcher = watcher

    def get_command(self, conventions):
        return self._UpdateExternalReplicationCommand(self._watcher)

    class _UpdateExternalReplicationCommand(RavenCommand):
        def __init__(self, watcher):
            if watcher is None:
                raise ValueError("watcher cannot be None")

            super(
                UpdateExternalReplicationOperation._UpdateExternalReplicationCommand,
                self,
            ).__init__(method="POST", is_raft_request=True)
            self._watcher = watcher

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/external-replication".format(
                server_node.url, server_node.database
            )
            self.data = {"Watcher": self._watcher}

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.InvalidOperationException(response["Message"], response["Type"], response["Error"])
            except ValueError:
                raise response.raise_for_status()
            return {"raft_command_index": response["RaftCommandIndex"]}


class PutConnectionStringOperation(MaintenanceOperation):
    def __init__(self, connection_string_def):

        if connection_string_def is None:
            raise ValueError("connection_string_def cannot be None")

        super(PutConnectionStringOperation, self).__init__()
        self._connection_string_def = connection_string_def

    def get_command(self, conventions):
        return self._PutConnectionStringCommand(self._connection_string_def)

    class _PutConnectionStringCommand(RavenCommand):
        def __init__(self, connection_string_def):
            if connection_string_def is None:
                raise ValueError("connection_string_def cannot be None")

            super(PutConnectionStringOperation._PutConnectionStringCommand, self).__init__(
                method="PUT", is_raft_request=True
            )
            self._connection_string_def = connection_string_def

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/connection-strings".format(server_node.url, server_node.database)
            self.data = self._connection_string_def

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.InvalidOperationException(response["Message"], response["Type"], response["Error"])
            except ValueError:
                raise response.raise_for_status()
            return {"raft_command_index": response["RaftCommandIndex"]}


class DeleteIndexOperation(MaintenanceOperation):
    def __init__(self, index_name):
        super(DeleteIndexOperation, self).__init__()
        self._index_name = index_name

    def get_command(self, conventions):
        return self._DeleteIndexCommand(self._index_name)

    class _DeleteIndexCommand(RavenCommand):
        def __init__(self, index_name):
            if not index_name:
                raise ValueError("Invalid index_name")
            super(DeleteIndexOperation._DeleteIndexCommand, self).__init__(method="DELETE")
            self._index_name = index_name

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes?name={2}".format(
                server_node.url, server_node.database, Utils.quote_key(self._index_name)
            )

        def set_response(self, response):
            pass


class GetIndexOperation(MaintenanceOperation):
    def __init__(self, index_name):
        """
        @param str index_name: The name of the index
        @returns The IndexDefinition of the index
        :rtype IndexDefinition
        """
        super(GetIndexOperation, self).__init__()
        self._index_name = index_name

    def get_command(self, conventions):
        return self._GetIndexCommand(self._index_name)

    class _GetIndexCommand(RavenCommand):
        def __init__(self, index_name):
            """
            @param str index_name: Name of the index you like to get or delete
            """
            super(GetIndexOperation._GetIndexCommand, self).__init__(method="GET", is_read_request=True)
            if index_name is None:
                raise AttributeError("index_name")
            self._index_name = index_name

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes?{2}".format(
                server_node.url,
                server_node.database,
                "name={0}".format(Utils.quote_key(self._index_name, True)),
            )

        def set_response(self, response):
            if response is None:
                return None
            data = {}
            try:
                response = response.json()["Results"]
                if len(response) > 1:
                    raise ValueError("response is Invalid")
                for key, value in response[0].items():
                    data[Utils.convert_to_snake_case(key)] = value
                return IndexDefinition(**data)

            except ValueError:
                raise response.raise_for_status()


class GetIndexesOperation(MaintenanceOperation):
    def __init__(self, start: int, page_size: int):
        super(GetIndexesOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetIndexesCommand(self._start, self._page_size)

    class _GetIndexesCommand(RavenCommand):
        def __init__(self, start: int, page_size: int):
            super().__init__(method="GET", is_read_request=True)
            self.start = start
            self.page_size = page_size

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes?start={self.start}&pageSize{self.page_size} "
            )

        def set_response(self, response):
            response = response.json()["Results"]
            result = []
            for index in response:
                data = {}
                for key, value in index.items():
                    data[Utils.convert_to_snake_case(key)] = value
                result.append(IndexDefinition(**data))
            return result


class GetIndexingStatusOperation(MaintenanceOperation):
    def __init__(self):
        super(GetIndexingStatusOperation, self).__init__()

    def get_command(self, conventions):
        return self._GetIndexingStatusCommand()

    class _GetIndexingStatusCommand(RavenCommand):
        def __init__(self):
            super(GetIndexingStatusOperation._GetIndexingStatusCommand, self).__init__(
                method="GET", is_read_request=True
            )

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/status"

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            return response


class GetIndexNamesOperation(MaintenanceOperation):
    def __init__(self, start, page_size):
        super(GetIndexNamesOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetIndexNamesCommand(self._start, self._page_size)

    class _GetIndexNamesCommand(RavenCommand):
        def __init__(self, start, page_size):
            super(GetIndexNamesOperation._GetIndexNamesCommand, self).__init__(method="GET", is_read_request=True)
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes?start={2}&pageSize={3}&namesOnly=true".format(
                server_node.url, server_node.database, self._start, self._page_size
            )

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")

            return response["Results"]


class PutIndexesOperation(MaintenanceOperation):
    def __init__(self, *indexes_to_add):
        if len(indexes_to_add) == 0:
            raise ValueError("Invalid indexes_to_add")

        super(PutIndexesOperation, self).__init__()
        self._indexes_to_add = indexes_to_add

    def get_command(self, conventions):
        return self._PutIndexesCommand(*self._indexes_to_add)

    class _PutIndexesCommand(RavenCommand):
        def __init__(self, *index_to_add):
            """
            @param index_to_add: Index to add to the database
            :type args of IndexDefinition
            :rtype dict (etag, transformer)
            """
            super(PutIndexesOperation._PutIndexesCommand, self).__init__(method="PUT", is_raft_request=True)
            if index_to_add is None:
                raise ValueError("None indexes_to_add is not valid")

            self.indexes_to_add = []
            for index_definition in index_to_add:
                if not isinstance(index_definition, IndexDefinition):
                    raise ValueError("index_definition in indexes_to_add must be IndexDefinition type")
                if index_definition.name is None:
                    raise ValueError("None Index name is not valid")
                self.indexes_to_add.append(index_definition.to_json())

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/indexes".format(server_node.url, server_node.database)
            self.data = {"Indexes": self.indexes_to_add}

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Error"])
                return response["Results"]
            except ValueError:
                response.raise_for_status()


class DisableIndexOperation(MaintenanceOperation):
    def __init__(self, name: str):
        super().__init__()
        self._index_name = name

    def get_command(self, conventions):
        return self._DisableIndexCommand(self._index_name)

    class _DisableIndexCommand(RavenCommand):
        def __init__(self, index_name: str):
            self._index_name = index_name
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}/admin/indexes"
                f"/disable?name={Utils.escape(self._index_name, False, False)}"
            )

        def set_response(self, response):
            pass


class EnableIndexOperation(MaintenanceOperation):
    def __init__(self, name: str):
        super().__init__()
        self._index_name = name

    def get_command(self, conventions):
        return self._EnableIndexCommand(self._index_name)

    class _EnableIndexCommand(RavenCommand):
        def __init__(self, index_name: str):
            self._index_name = index_name
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}/admin/indexes"
                f"/enable?name={Utils.escape(self._index_name, False, False)}"
            )

        def set_response(self, response):
            pass


class StopIndexingOperation(MaintenanceOperation):
    def __init__(self):
        super().__init__()

    def get_command(self, conventions):
        return self._StopIndexingCommand()

    class _StopIndexingCommand(RavenCommand):
        def __init__(self):
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/admin/indexes/stop"

        def set_response(self, response):
            pass


class StartIndexingOperation(MaintenanceOperation):
    def __init__(self):
        super().__init__()

    def get_command(self, conventions):
        return self._StartIndexingCommand()

    class _StartIndexingCommand(RavenCommand):
        def __init__(self):
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/admin/indexes/start"

        def set_response(self, response):
            pass


class StartIndexOperation(MaintenanceOperation):
    def __init__(self, index_name):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        super(StartIndexOperation, self).__init__()
        self._index_name = index_name

    def get_command(self, conventions):
        return self._StartIndexCommand(self._index_name)

    class _StartIndexCommand(RavenCommand):
        def __init__(self, index_name):
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}"
                f"/admin/indexes/start?name={Utils.quote_key(self._index_name)}"
            )

        def set_response(self, response):
            pass


class StopIndexOperation(MaintenanceOperation):
    def __init__(self, index_name):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        super(StopIndexOperation, self).__init__()
        self._index_name = index_name

    def get_command(self, conventions):
        return self._StopIndexCommand(self._index_name)

    class _StopIndexCommand(RavenCommand):
        def __init__(self, index_name):
            super().__init__(method="POST")
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}"
                f"/admin/indexes/stop?name={Utils.quote_key(self._index_name)}"
            )

        def set_response(self, response):
            pass


class GetIndexStatisticsOperation(MaintenanceOperation):
    def __init__(self, name):
        super(GetIndexStatisticsOperation, self).__init__()
        self._index_name = name

    def get_command(self, conventions):
        return self._GetIndexStatisticsCommand(self._index_name)

    class _GetIndexStatisticsCommand(RavenCommand):
        def __init__(self, index_name: str):
            super().__init__(method="GET")
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes/stats?name={Utils.escape(self._index_name, False, False)}"
            )

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")

            return response["Results"]


class GetIndexesStatisticsOperation(MaintenanceOperation):
    def __init__(self):
        super(GetIndexesStatisticsOperation, self).__init__()

    def get_command(self, conventions):
        return self._GetIndexesStatisticsCommand()

    class _GetIndexesStatisticsCommand(RavenCommand):
        def __init__(self):
            super().__init__(method="GET", is_read_request=True)

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/stats"

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")

            return response["Results"]


class GetStatisticsOperation(MaintenanceOperation):
    def __init__(self, debug_tag: str = None, node_tag: str = None):
        super(GetStatisticsOperation, self).__init__()
        self._debug_tag = debug_tag
        self._node_tag = node_tag

    def get_command(self, conventions):
        return GetStatisticsCommand(self._debug_tag, self._node_tag)


class GetTermsOperation(MaintenanceOperation):
    def __init__(self, index_name: str, field: str, from_value: str, page_size: int = None):
        if index_name is None:
            raise ValueError("Index name cannot be None")
        if field is None:
            raise ValueError("Field cannot be null")
        super(GetTermsOperation, self).__init__()
        self._index_name = index_name
        self._field = field
        self._from_value = from_value
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetTermsCommand(self._index_name, self._field, self._from_value, self._page_size)

    class _GetTermsCommand(RavenCommand):
        def __init__(self, index_name: str, field: str, from_value: str, page_size: int):
            self._index_name = index_name
            self._field = field
            self._from_value = from_value
            self._page_size = page_size
            super().__init__(method="GET", is_read_request=True)

        def create_request(self, server_node):
            self.url = (
                f"{server_node.url}/databases/{server_node.database}"
                f"/indexes/terms?name={Utils.escape(self._index_name, False, False)}"
                f"&field={Utils.escape(self._field, False, False)}"
                f"&fromValue={self._from_value if self._from_value is not None else ''}"
                f"&pageSize={self._page_size if self._page_size is not None else ''}"
            )

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")
            response = response.json()
            return response["Terms"]


class GetCollectionStatisticsOperation(MaintenanceOperation):
    def get_command(self, conventions):
        return self._GetCollectionStatisticsCommand()

    class _GetCollectionStatisticsCommand(RavenCommand):
        def __init__(self):
            super().__init__(method="GET", is_read_request=True)

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/collections/stats"

        def set_response(self, response):
            if response and response.status_code == 200:
                return response.json()
            return None


class GetIndexErrorsOperation(MaintenanceOperation):
    def __init__(self, index_names: Optional[Union[List[str], Tuple[str], Set[str]]] = None):
        super(GetIndexErrorsOperation, self).__init__()
        self._index_names = index_names

    def get_command(self, conventions):
        return self._GetIndexErrorsCommand(self._index_names)

    class _GetIndexErrorsCommand(RavenCommand):
        def __init__(self, index_names):
            super().__init__(method="GET", is_read_request=True)
            self._index_names = index_names

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/errors"
            if self._index_names and len(self._index_names) != 0:
                self.url += "?"
                for index_name in self._index_names:
                    self.url += f"&name={Utils.escape(index_name, None, None)}"

        def set_response(self, response):
            if response and response.status_code == 200:
                response = response.json()
                if response.get("Results", False):
                    return [
                        IndexErrors(
                            entry["Name"],
                            [
                                IndexingError(
                                    error["Error"],
                                    Utils.string_to_datetime(error["Timestamp"]),
                                    error["Document"],
                                    error["Action"],
                                )
                                for error in entry["Errors"]
                            ],
                        )
                        for entry in response["Results"]
                    ]


class IndexHasChangedOperation(MaintenanceOperation):
    def __init__(self, index: IndexDefinition):
        if index is None:
            raise ValueError("Index cannot be null")
        super(IndexHasChangedOperation, self).__init__()
        self._index = index

    def get_command(self, conventions):
        return self._IndexHasChangedCommand(self._index)

    class _IndexHasChangedCommand(RavenCommand):
        def __init__(self, index: IndexDefinition):
            super().__init__(method="POST", is_read_request=False)
            self._index = index

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/has-changed"
            self.data = {
                "Configuration": self._index.configuration,
                "Fields": self._index.fields,
                "LockMode": self._index.lock_mode,
                "Maps": self._index.maps,
                "Name": self._index.name,
                "OutputReduceToCollection": self._index.output_reduce_to_collection,
                "Priority": self._index.priority,
                "Reduce": self._index.reduce,
                "SourceType": self._index.source_type,
                "Type": self._index.type,
            }

        def set_response(self, response):
            if not response:
                raise ValueError("Response is invalid")
            response = response.json()
            return response["Changed"]


class SetIndexesLockOperation(MaintenanceOperation):
    def __init__(self, mode: IndexLockMode, *index_names: str):
        if len(index_names) == 0:
            raise ValueError("Index names count cannot be zero. Pass at least one index name.")
        super(SetIndexesLockOperation, self).__init__()
        self._mode = mode
        self._index_names = index_names
        self.__filter_auto_indexes()

    def get_command(self, conventions):
        return self._SetIndexesLockCommand(self._mode, self._index_names)

    def __filter_auto_indexes(self):
        for name in self._index_names:
            if name.startswith("auto/"):
                raise ValueError("Index list contains Auto-Indexes. Lock Mode is not set for Auto-Indexes")

    class _SetIndexesLockCommand(RavenCommand):
        def __init__(self, mode: IndexLockMode, index_names: Union[List[str], Tuple[str], Set[str]]):
            super().__init__(
                method="POST",
                is_raft_request=True,
            )
            self._mode = mode
            self._index_names = index_names

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/set-lock"
            self.data = {"IndexNames": self._index_names, "Mode": self._mode}

        def set_response(self, response):
            pass


class SetIndexesPriorityOperation(MaintenanceOperation):
    def __init__(self, priority: IndexPriority, *index_names: str):
        if len(index_names) == 0:
            raise ValueError("Index names count cannot be zero. Pass at least one index name.")
        super(SetIndexesPriorityOperation, self).__init__()
        self._priority = priority
        self._index_names = index_names

    def get_command(self, conventions):
        return self._SetIndexesPriorityCommand(self._priority, self._index_names)

    class _SetIndexesPriorityCommand(RavenCommand):
        def __init__(self, priority: IndexPriority, index_names: Union[List[str], Tuple[str], Set[str]]):
            super().__init__(
                method="POST",
                is_raft_request=True,
            )
            self._priority = priority
            self._index_names = index_names

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/indexes/set-priority"
            self.data = {"IndexNames": self._index_names, "Priority": self._priority}

        def set_response(self, response):
            pass
