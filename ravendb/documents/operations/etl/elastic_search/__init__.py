from typing import List

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor


class ApiKeyAuthentication:
    def __init__(self, api_key_id: str = None, api_key: str = None, encoded_api_key: str = None):
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.encoded_api_key = encoded_api_key


class BasicAuthentication:
    def __init__(self, username: str = None, password: str = None):
        self.username = username
        self.password = password


class CertificateAuthentication:
    def __init__(self, certificates_base64: List[str] = None):
        self.certificates_base64 = certificates_base64


class Authentication:
    def __init__(self, api_key: ApiKeyAuthentication = None, basic: BasicAuthentication = None, certificate: CertificateAuthentication = None):
        self.api_key = api_key
        self.basic = basic
        self.certificate = certificate


class ElasticSearchConnectionString(ConnectionString):
    def __init__(self, name: str, nodes: List[str] = None, authentication: Authentication = None):
        self.name = name
        self.nodes = nodes
        self.authentication = authentication

    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.ELASTIC_SEARCH.value

    def to_json(self):
        return {
            "Name": self.name,
            "Nodes": self.nodes,
            "Authentication": self.authentication,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.ELASTIC_SEARCH,
        }