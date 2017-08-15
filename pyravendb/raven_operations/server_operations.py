from pyravendb.commands.raven_commands import RavenCommand, GetDatabaseTopologyCommand
from pyravendb.data.certificate import CertificateDefinition
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


class GetDatabaseTopologyOperation(ServerOperation):
    def __init__(self, database_name):
        super(GetDatabaseTopologyOperation, self).__init__()
        self._database_name = database_name

    def get_command(self, conventions):
        return GetDatabaseTopologyCommand(self._database_name)


class GetCertificateOperation(ServerOperation):
    def __init__(self, name):
        if name is None:
            raise ValueError("name is Invalid cannot be None")
        super(GetCertificateOperation, self).__init__()
        self._name = name

    def get_command(self, conventions):
        return self._GetCertificateCommand(self._name)

    class _GetCertificateCommand(RavenCommand):
        def __init__(self, name):
            super(GetCertificateOperation._GetCertificateCommand, self).__init__(method="GET")
            self._name = name

        def create_request(self, server_node):
            self.url = "{0}/admin/certificates?name={1}".format(server_node.url, Utils.quote_key(self._name))

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
    def __init__(self, name, permissions, server_admin=False, password=None):
        """
        Add certificate json to the server and get certificate from server to use
        @param name: The name of the certificate
        :type str
        @param permissions: the permissions to the client
        :type dict [str:DatabaseAccess]
        @param server_admin: If we server admin
        :type bool
        @param password: The password of the certificate
        :type str
        """
        if name is None:
            raise ValueError("name cannot by None")
        if permissions is None:
            raise ValueError("permissions cannot be None")

        super(CreateClientCertificateOperation, self).__init__()
        self._name = name
        self._permissions = permissions
        self._server_admin = server_admin
        self._password = password

    def get_command(self, conventions):
        return self._CreateClientCertificateCommand(self._name, self._permissions, self._server_admin, self._password)

    class _CreateClientCertificateCommand(RavenCommand):
        def __init__(self, name, permissions, server_admin, password):
            if name is None:
                raise ValueError("name cannot by None")
            if permissions is None:
                raise ValueError("permissions cannot be None")

            super(CreateClientCertificateOperation._CreateClientCertificateCommand, self).__init__(method="POST",
                                                                                                   is_read_request=True,
                                                                                                   use_stream=True)
            self._name = name
            self._permissions = permissions
            self._server_admin = server_admin
            self._password = password

        def create_request(self, server_node):
            self.url = server_node.url + "/admin/certificates"
            self.data = {"Name": self._name, "ServerAdmin": self._server_admin}
            if self._password:
                self.data["Password"] = self._password

            permissions = []
            for key, value in self._permissions.items():
                permissions.append({"Database": key, "Access": str(self.value)})

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
    def __init__(self, certificate, permissions, server_admin=False):
        """
        @param certificate: X509 certificate file (OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, pem))
        :type X509
        @param permissions: the permissions to the client
        :type dict [str:DatabaseAccess]
        @param server_admin: If we server admin
        :type bool
        """
        if certificate is None:
            raise ValueError("certificate cannot be None")
        if permissions is None:
            raise ValueError("permissions cannot be None")

        super(PutClientCertificateOperation, self).__init__()
        self._certificate = certificate
        self._permissions = permissions
        self._server_admin = server_admin

    def get_command(self, conventions):
        return self._PutClientCertificateCommand(self._certificate, self._permissions, self._server_admin)

    class _PutClientCertificateCommand(RavenCommand):
        def __init__(self, certificate, permissions, server_admin=False):
            if certificate is None:
                raise ValueError("certificate cannot be None")
            if permissions is None:
                raise ValueError("permissions cannot be None")

            super(PutClientCertificateOperation._PutClientCertificateCommand, self).__init__(method="PUT",
                                                                                             use_stream=True)
            self._certificate = certificate
            self._permissions = permissions
            self._server_admin = server_admin

        def create_request(self, server_node):
            self.url = server_node.url + "/admin/certificates"

            self.data = {"Certificate": self._certificate, "ServerAdmin": self._server_admin}
            permissions = []
            for key, value in self._permissions.items():
                permissions.append({"Database": key, "Access": str(self.value)})

            self.data["Permissions"] = permissions

        def set_response(self, response):
            pass


class PutCertificateOperation(ServerOperation):
    def __init__(self, name, certificate):
        """
        @param name: The name of the certificate
        :type str
        @param certificate: The certificate to add
        :type CertificateDefinition
        """
        if name is None:
            raise ValueError("certificate cannot be None")
        if certificate is None:
            raise ValueError("permissions cannot be None")

        super(PutCertificateOperation, self).__init__()
        self._name = name
        self._certificate = certificate

    def get_command(self, conventions):
        return self._PutCertificateCommand(self._name, self._certificate)

    class _PutCertificateCommand(RavenCommand):
        def __init__(self, name, certificate):
            if name is None:
                raise ValueError("certificate cannot be None")
            if certificate is None:
                raise ValueError("permissions cannot be None")

            super(PutCertificateOperation._PutCertificateCommand, self).__init__(method="PUT")
            self._name = name
            self._certificate = certificate

        def create_request(self, server_node):
            self.url = "{0}/admin/certificates?name={1}".format(server_node.url, Utils.quote_key(self._name))
            self.data = self._certificate.to_json()

        def set_response(self, response):
            pass


class DeleteCertificateOperation(ServerOperation):
    def __init__(self, name):
        if name is None:
            raise ValueError("certificate cannot be None")
        super(DeleteCertificateOperation, self).__init__()
        self._name = name

    def get_command(self, conventions):
        return self._DeleteCertificateCommand(self._name)

    class _DeleteCertificateCommand(RavenCommand):
        def __init__(self, name):
            if name is None:
                raise ValueError("certificate cannot be None")
            super(DeleteCertificateOperation._DeleteCertificateCommand, self).__init__(method="DELETE")
            self._name = name

        def create_request(self, server_node):
            self.url = "{0}/admin/certificates?name={1}".format(server_node.url, Utils.quote_key(self._name))

        def set_response(self, response):
            pass
