from typing import List, Dict, Any

from ravendb.documents.operations.connection_strings import ConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType


class ApiKeyAuthentication:
    def __init__(self, api_key_id: str = None, api_key: str = None, encoded_api_key: str = None):
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.encoded_api_key = encoded_api_key

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKeyId": self.api_key_id,
            "ApiKey": self.api_key,
            "EncodedApiKey": self.encoded_api_key,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "ApiKeyAuthentication":
        return cls(
            api_key_id=data.get("ApiKeyId") or data.get("api_key_id"),
            api_key=data.get("ApiKey") or data.get("api_key"),
            encoded_api_key=data.get("EncodedApiKey") or data.get("encoded_api_key"),
        )


class BasicAuthentication:
    def __init__(self, username: str = None, password: str = None):
        self.username = username
        self.password = password

    def to_json(self) -> Dict[str, Any]:
        return {
            "Username": self.username,
            "Password": self.password,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BasicAuthentication":
        return cls(
            username=data.get("Username") or data.get("username"),
            password=data.get("Password") or data.get("password"),
        )


class CertificateAuthentication:
    def __init__(self, certificates_base64: List[str] = None):
        self.certificates_base64 = certificates_base64

    def to_json(self) -> Dict[str, Any]:
        return {
            "CertificatesBase64": list(self.certificates_base64) if self.certificates_base64 is not None else None,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "CertificateAuthentication":
        certs = data.get("CertificatesBase64") or data.get("certificates_base64")
        return cls(certificates_base64=list(certs) if certs is not None else None)


class Authentication:
    def __init__(
        self,
        api_key: ApiKeyAuthentication = None,
        basic: BasicAuthentication = None,
        certificate: CertificateAuthentication = None,
    ):
        self.api_key = api_key
        self.basic = basic
        self.certificate = certificate

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key.to_json() if self.api_key is not None else None,
            "Basic": self.basic.to_json() if self.basic is not None else None,
            "Certificate": self.certificate.to_json() if self.certificate is not None else None,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Authentication":
        api_key_data = data.get("ApiKey")
        basic_data = data.get("Basic")
        certificate_data = data.get("Certificate")

        return cls(
            api_key=ApiKeyAuthentication.from_json(api_key_data) if api_key_data else None,
            basic=BasicAuthentication.from_json(basic_data) if basic_data else None,
            certificate=CertificateAuthentication.from_json(certificate_data) if certificate_data else None,
        )


class ElasticSearchConnectionString(ConnectionString):
    def __init__(self, name: str, nodes: List[str] = None, authentication: Authentication = None):
        super().__init__(name)
        self.nodes = nodes
        self.authentication = authentication

    @property
    def get_type(self):
        return ConnectionStringType.ELASTIC_SEARCH.value

    def to_json(self):
        return {
            "Name": self.name,
            "Nodes": self.nodes,
            "Authentication": self.authentication.to_json() if self.authentication else None,
            "Type": ConnectionStringType.ELASTIC_SEARCH,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Any:
        return cls(
            name=json_dict["Name"],
            nodes=json_dict["Nodes"],
            authentication=(
                Authentication.from_json(json_dict["Authentication"]) if json_dict["Authentication"] else None
            ),
        )
