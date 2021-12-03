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
