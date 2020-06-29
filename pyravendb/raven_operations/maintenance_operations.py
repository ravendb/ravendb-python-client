from pyravendb.commands.raven_commands import RavenCommand, GetStatisticsCommand
from pyravendb.tools.utils import Utils
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexDefinition
from abc import abstractmethod


class MaintenanceOperation(object):
    __slots__ = ['__operation']

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
    def __init__(self, name, connection_string_name, mentor_node=None, delayed_replication_for=None):
        self.DelayedReplicationFor = delayed_replication_for
        self.Name = name
        self.ConnectionStringName = connection_string_name
        self.MentorNode = mentor_node


class PullReplicationAsSink:
    def __init__(self, hub, connection_string_name, certificate_base64, certificate_password, mentor_node=None, delayed_replication_for=None):
        self.DelayedReplicationFor = delayed_replication_for
        self.HubDefinitionName = hub
        self.ConnectionStringName = connection_string_name
        self.MentorNode = mentor_node


class ConnectionString:

    @staticmethod
    def raven(name,database, urls):
        return {"Type": "Raven", "Name": name, "Database": database, "TopologyDiscoveryUrls": urls}

    @staticmethod
    def sql(name, factory, connection_string):
        return {"Type": "Raven", "FactoryName": factory, "ConnectionString": connection_string}


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

            super(UpdatePullReplicationAsSinkOperation._UpdatePullReplicationAsSinkCommand, self).__init__(method="POST",
                                                                                                   is_raft_request=True)
            self._definition = definition

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/sink-pull-replication".format(server_node.url,
                                                                                    server_node.database)
            self.data = { "PullReplicationAsSink": self._definition }

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.InvalidOperationException(response["Message"], response["Type"],
                                                               response["Error"])
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

            super(PutPullReplicationAsHubOperation._PutPullReplicationAsHubCommand, self).__init__(method="PUT",
                                                                                                   is_raft_request=True)
            self._definition = definition

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/pull-replication/hub".format(server_node.url, server_node.database)
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

            super(UpdateExternalReplicationOperation._UpdateExternalReplicationCommand, self).__init__(method="POST",
                                                                                                       is_raft_request=True)
            self._watcher = watcher

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/admin/tasks/external-replication".format(server_node.url, server_node.database)
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

            super(PutConnectionStringOperation._PutConnectionStringCommand, self).__init__(method="PUT",
                                                                                           is_raft_request=True)
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
            self.url = "{0}/databases/{1}/indexes?name={2}".format(server_node.url, server_node.database,
                                                                   Utils.quote_key(self._index_name))

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
            self.url = "{0}/databases/{1}/indexes?{2}".format(server_node.url, server_node.database,
                                                              "name={0}".format(
                                                                  Utils.quote_key(self._index_name, True)))

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
            self.url = "{0}/databases/{1}/indexes?start={2}&pageSize={3}&namesOnly=true".format(server_node.url,
                                                                                                server_node.database,
                                                                                                self._start,
                                                                                                self._page_size)

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
        self._index_name = index_name
        super().__init__(self._index_name)

    def get_command(self, conventions):
        return self._StartIndexCommand(self._index_name)

    class _StartIndexCommand(RavenCommand):
        def __init__(self, index_name):
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/admin/indexes/start?name={Utils.quote_key(self._index_name)}"

        def set_response(self, response):
            pass


class StopIndexOperation(MaintenanceOperation):
    def __init__(self, index_name):
        if not index_name:
            raise ValueError("Index name can't be None or empty")
        self._index_name = index_name
        super().__init__(self._index_name)

    def get_command(self, conventions):
        return self._StopIndexCommand(self._index_name)

    class _StopIndexCommand(RavenCommand):
        def __init__(self, index_name):
            if not index_name:
                raise ValueError("Index name can't be None or empty")
            self._index_name = index_name
            super().__init__(method="POST")

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/admin/indexes/stop?name={Utils.quote_key(self._index_name)}"

        def set_response(self, response):
            pass


class GetStatisticsOperation(MaintenanceOperation):
    def get_command(self, conventions):
        return GetStatisticsCommand()
