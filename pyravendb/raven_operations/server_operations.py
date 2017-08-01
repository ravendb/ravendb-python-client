from pyravendb.commands.raven_commands import RavenCommand
from pyravendb.custom_exceptions import exceptions
from requests.exceptions import RequestException
from abc import abstractmethod


class ServerOperation(object):
    __slots__ = ['__operation']

    def __init__(self):
        self.__operation = "ServerOperation"

    @property
    def operation(self):
        return self.__operation

    @abstractmethod
    def get_command(self, conventions):
        raise NotImplementedError


class CreateDatabaseOperation(ServerOperation):
    def __init__(self, database_name, replication_factor=1, settings=None, secure_settings=None):
        super(CreateDatabaseOperation, self).__init__()
        self.replication_factor = replication_factor
        self.database_record = self.get_default_database_record()
        self.database_record["DatabaseName"] = database_name
        if settings:
            self.database_record["Settings"] = settings
        if secure_settings:
            self.database_record["SecuredSettings"] = secure_settings

    def get_command(self, conventions):
        return self._CreateDatabaseCommand(self.database_record, self.replication_factor, conventions)

    def get_default_database_record(self):
        return {"DatabaseName": None, "Disabled": False, "Encrypted": False, "DeletionInProgress": None,
                "DataDirectory": None, "Topology": None,
                "ConflictSolverConfig": {"DatabaseResolverId": None, "ResolveByCollection": None,
                                         "ResolveToLatest": False},
                "Indexes": None, "AutoIndexes": None, "Transformers": None, "Identities": None, "Settings": {},
                "ExternalReplication": [], "RavenConnectionStrings": {}, "SqlConnectionStrings": {}, "RavenEtls": None,
                "SqlEtls": None, "SecuredSettings": None, "Revisions": None, "Expiration": None,
                "PeriodicBackups": None, "Client": None, "CustomFunctions": None}

    class _CreateDatabaseCommand(RavenCommand):
        def __init__(self, database_record, replication_factor, convention):
            if convention is None:
                raise ValueError("Invalid convention")
            super(CreateDatabaseOperation._CreateDatabaseCommand, self).__init__(method="PUT")

            self._database_record = database_record
            self._replication_factor = replication_factor
            self.convention = convention

        def create_request(self, server_node):
            self.url = "{0}/admin/databases?name={1}".format(server_node.url, self._database_record["DatabaseName"])
            self.url += "&replication-factor=" + str(self._replication_factor)

            self.data = self._database_record

        def set_response(self, response):
            if response is None:
                raise ValueError("response is invalid.")

            if response.status_code == 201:
                return response.json()

            if response.status_code == 400:
                response = response.json()
                if "Error" in response:
                    raise RequestException(response["Error"])


class DeleteDatabaseOperation(ServerOperation):
    def __init__(self, database_name, hard_delete, from_node=None):
        if database_name is None:
            raise ValueError("Invalid database_name")

        super(DeleteDatabaseOperation, self).__init__()
        self._database_name = database_name
        self._hard_delete = hard_delete
        self._from_node = from_node

    def get_command(self, conventions):
        return self._DeleteDatabaseCommand(self._database_name, self._hard_delete, self._from_node)

    class _DeleteDatabaseCommand(RavenCommand):
        def __init__(self, database_name, hard_delete, from_node):
            if database_name is None:
                raise ValueError("Invalid database_name")

            super(DeleteDatabaseOperation._DeleteDatabaseCommand, self).__init__(method="Delete")
            self._database_name = database_name
            self._hard_delete = hard_delete
            self._from_node = from_node

        def create_request(self, server_node):
            self.url = "{0}/admin/databases?name={1}".format(server_node.url, self._database_name)
            if self._hard_delete:
                self.url += "&hard-delete=true"
            if self._from_node:
                self.url += "&from-node=" + self._from_node

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.DatabaseDoesNotExistException(response["Message"])
            except ValueError:
                raise response.raise_for_status()


class GetDatabaseNamesOperation(ServerOperation):
    def __init__(self, start, page_size):
        super(GetDatabaseNamesOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetDatabaseNamesCommand(self._start, self._page_size)

    class _GetDatabaseNamesCommand(RavenCommand):
        def __init__(self, start, page_size):
            super(GetDatabaseNamesOperation._GetDatabaseNamesCommand, self).__init__(method="GET", is_read_request=True)
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = "{0}/databases?start={1}&pageSize={2}&namesOnly=true".format(server_node.url, self._start,
                                                                                    self._page_size)

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])

            if "Databases" not in response:
                raise ValueError("Invalid response")

                return response["Databases"]
