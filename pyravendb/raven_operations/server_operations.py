from pyravendb.commands.raven_commands import RavenCommand, GetDatabaseRecordCommand
from pyravendb.data.certificate import CertificateDefinition, SecurityClearance
from pyravendb.custom_exceptions import exceptions
from requests.exceptions import RequestException
from pyravendb.tools.utils import Utils
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
        self.database_record = CreateDatabaseOperation.get_default_database_record()
        Utils.database_name_validation(database_name)
        self.database_record["DatabaseName"] = database_name
        if settings:
            self.database_record["Settings"] = settings
        if secure_settings:
            self.database_record["SecuredSettings"] = secure_settings

    def get_command(self, conventions):
        return self._CreateDatabaseCommand(self.database_record, self.replication_factor, conventions)

    @staticmethod
    def get_default_database_record():
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
            super(CreateDatabaseOperation._CreateDatabaseCommand, self).__init__(method="PUT", is_raft_request=True)

            self._database_record = database_record
            self._replication_factor = replication_factor
            self.convention = convention

        def create_request(self, server_node):
            self.url = "{0}/admin/databases?name={1}".format(server_node.url, self._database_record["DatabaseName"])
            self.url += "&replicationFactor=" + str(self._replication_factor)

            self.data = self._database_record

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            try:
                response = response.json()
                if "Error" in response:
                    raise RequestException(response["Message"], response["Type"])
            except ValueError:
                raise response.raise_for_status()


class DeleteDatabaseOperation(ServerOperation):
    def __init__(self, database_name, hard_delete, from_node=None, time_to_wait_for_confirmation=None):
        if database_name is None:
            raise ValueError("Invalid database_name")

        super(DeleteDatabaseOperation, self).__init__()
        self._parameters = {"DatabaseNames": [database_name], "HardDelete": hard_delete,
                            "TimeToWaitForConfirmation": time_to_wait_for_confirmation}
        if from_node:
            self._parameters["FromNodes"] = [from_node]

    def get_command(self, conventions):
        return self._DeleteDatabaseCommand(self._parameters)

    class _DeleteDatabaseCommand(RavenCommand):
        def __init__(self, parameters):
            if parameters is None:
                raise ValueError("Invalid parameters")

            self._parameters = parameters

            super(DeleteDatabaseOperation._DeleteDatabaseCommand, self).__init__(method="DELETE", is_raft_request=True)

        def create_request(self, server_node):
            self.url = "{0}/admin/databases".format(server_node.url)
            self.data = self._parameters

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.DatabaseDoesNotExistException(response["Message"], response["Type"])
            except ValueError:
                raise response.raise_for_status()
            return {"raft_command_index": response["RaftCommandIndex"]}


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


class GetDatabaseRecordOperation(ServerOperation):
    def __init__(self, database_name):
        super(GetDatabaseRecordOperation, self).__init__()
        self._database_name = database_name

    def get_command(self, conventions):
        return GetDatabaseRecordCommand(self._database_name)


# --------------------CertificateOperation------------------------------

class GetCertificateOperation(ServerOperation):
    def __init__(self, start, page_size):

        super(GetCertificateOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetCertificateCommand(self._start, self._page_size)

    class _GetCertificateCommand(RavenCommand):
        def __init__(self, start, page_size):
            super(GetCertificateOperation._GetCertificateCommand, self).__init__(method="GET")
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = "{0}/admin/certificates?start={1}&pageSize={2}".format(server_node.url, self._start,
                                                                              self._page_size)

        def set_response(self, response):
            if response is None:
                raise ValueError("response is invalid.")
            data = {}
            try:
                response = response.json()["Results"]
                if len(response) > 1:
                    raise ValueError("response is Invalid")
                for key, value in response[0].items():
                    data[Utils.convert_to_snake_case(key)] = value
                return CertificateDefinition(**data)

            except ValueError:
                raise response.raise_for_status()


class CreateClientCertificateOperation(ServerOperation):
    def __init__(self, name, permissions, clearance=SecurityClearance.unauthenticated_clients, password=None):
        """
        Add certificate json to the server and get certificate from server to use
        @param str name: The name of the certificate
        @param Dict[str:DatabaseAccess] permissions: the permissions to the database the key is the name of the database
        the value is database access (read or admin)
        @param SecurityClearance clearance: The clearance of the client
        @param str password: The password of the certificate
        """
        if name is None:
            raise ValueError("name cannot be None")
        if permissions is None:
            raise ValueError("permissions cannot be None")

        super(CreateClientCertificateOperation, self).__init__()
        self._name = name
        self._permissions = permissions
        self._clearance = clearance
        self._password = password

    def get_command(self, conventions):
        return self._CreateClientCertificateCommand(self._name, self._permissions, self._clearance, self._password)

    class _CreateClientCertificateCommand(RavenCommand):
        def __init__(self, name, permissions, clearance, password):
            if name is None:
                raise ValueError("name cannot be None")
            if permissions is None:
                raise ValueError("permissions cannot be None")

            super(CreateClientCertificateOperation._CreateClientCertificateCommand, self).__init__(method="POST",
                                                                                                   is_read_request=True,
                                                                                                   use_stream=True)
            self._name = name
            self._permissions = permissions
            self._clearance = clearance
            self._password = password

        def create_request(self, server_node):
            self.url = server_node.url + "/admin/certificates"
            self.data = {"Name": self._name, "SecurityClearance": str(self._clearance)}
            if self._password:
                self.data["Password"] = self._password

            permissions = {}
            for key, value in self._permissions.items():
                permissions.update({"Database": key, "Access": str(self.value)})

            self.data["Permissions"] = permissions

        def set_response(self, response):
            if response is None:
                return None

            if response.status_code != 201:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Error"])
            return response.raw.data


class PutClientCertificateOperation(ServerOperation):
    def __init__(self, name, certificate, permissions, clearance=SecurityClearance.unauthenticated_clients):
        """
        @param str name: Certificate name
        @param x509 certificate: X509 certificate file (OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, pem))
        @param Dict[str:DatabaseAccess] permissions: the permissions to the client
        @param SecurityClearance clearance: The clearance of the client
        """
        if certificate is None:
            raise ValueError("certificate cannot be None")
        if permissions is None:
            raise ValueError("permissions cannot be None")

        super(PutClientCertificateOperation, self).__init__()
        self._name = name
        self._certificate = certificate
        self._permissions = permissions
        self._clearance = clearance

    def get_command(self, conventions):
        return self._PutClientCertificateCommand(self._name, self._certificate, self._permissions, self._clearance)

    class _PutClientCertificateCommand(RavenCommand):
        def __init__(self, name, certificate, permissions, clearance=SecurityClearance.unauthenticated_clients):
            if certificate is None:
                raise ValueError("certificate cannot be None")
            if permissions is None:
                raise ValueError("permissions cannot be None")

            super(PutClientCertificateOperation._PutClientCertificateCommand, self).__init__(method="PUT",
                                                                                             use_stream=True)
            self._name = name
            self._certificate = certificate
            self._permissions = permissions
            self._clearance = clearance

        def create_request(self, server_node):
            self.url = server_node.url + "/admin/certificates"

            self.data = {"Name": self._name, "Certificate": self._certificate,
                         "SecurityClearance": str(self._clearance)}
            permissions = []
            for key, value in self._permissions.items():
                permissions.append({"Database": key, "Access": str(self.value)})

            self.data["Permissions"] = permissions

        def set_response(self, response):
            pass


class DeleteCertificateOperation(ServerOperation):
    def __init__(self, thumbprint):
        if thumbprint is None:
            raise ValueError("certificate cannot be None")
        super(DeleteCertificateOperation, self).__init__()
        self._thumbprint = thumbprint

    def get_command(self, conventions):
        return self._DeleteCertificateCommand(self._thumbprint)

    class _DeleteCertificateCommand(RavenCommand):
        def __init__(self, thumbprint):
            if thumbprint is None:
                raise ValueError("certificate cannot be None")
            super(DeleteCertificateOperation._DeleteCertificateCommand, self).__init__(method="DELETE")
            self._thumbprint = thumbprint

        def create_request(self, server_node):
            self.url = "{0}/admin/certificates?thumbprint={1}".format(server_node.url,
                                                                      Utils.quote_key(self._thumbprint))

        def set_response(self, response):
            pass
