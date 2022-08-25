from __future__ import annotations

import base64
import enum
import json
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List

import requests

from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ServerOperation, VoidServerOperation
from ravendb.tools.utils import CaseInsensitiveDict, Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class SecurityClearance(enum.Enum):
    UNAUTHENTICATED_CLIENTS = "UnauthenticatedClients"
    CLUSTER_ADMIN = "ClusterAdmin"
    CLUSTER_NODE = "ClusterNode"
    OPERATOR = "Operator"
    VALID_USER = "ValidUser"

    def __str__(self):
        return self.value


class DatabaseAccess(enum.Enum):
    READ_WRITE = "ReadWrite"
    ADMIN = "Admin"
    READ = "Read"

    def __str__(self):
        return self.value


class CertificateRawData:
    def __init__(self, raw_data: bytes = None):
        self.raw_data = raw_data


class CertificateMetadata:
    def __init__(
        self,
        name: str = None,
        security_clearance: SecurityClearance = None,
        thumbprint: str = None,
        not_after: datetime = None,
        permissions: CaseInsensitiveDict[str, DatabaseAccess] = None,
        collection_secondary_keys: List[str] = None,
        collection_primary_key: str = None,
        public_key_pinning_hash: str = None,
    ):
        self.name = name
        self.security_clearance = security_clearance
        self.thumbprint = thumbprint
        self.not_after = not_after
        self.permissions = permissions
        self.collection_primary_key = collection_primary_key
        self.collection_secondary_keys = collection_secondary_keys
        self.public_key_pinning_hash = public_key_pinning_hash

    @classmethod
    def from_json(cls, json_dict: dict) -> CertificateMetadata:
        return cls(
            json_dict["Name"],
            SecurityClearance(json_dict.get("SecurityClearance", None)),
            json_dict.get("Thumbprint", None),
            Utils.string_to_datetime(json_dict["NotAfter"]) if "NotAfter" in json_dict else None,
            json_dict.get("Permissions", None),
            json_dict.get("CollectionSecondaryKeys", None),
            json_dict.get("CollectionPrimaryKey", None),
            json_dict.get("PublicKeyPinningHash", None),
        )


class CertificateDefinition(CertificateMetadata):
    def __init__(
        self,
        certificate: str = None,
        password: str = None,
        name: str = None,
        security_clearance: SecurityClearance = None,
        thumbprint: str = None,
        not_after: datetime = None,
        permissions: CaseInsensitiveDict[str, DatabaseAccess] = None,
        collection_secondary_keys: List[str] = None,
        collection_primary_key: str = None,
        public_key_pinning_hash: str = None,
    ):
        super().__init__(
            name,
            security_clearance,
            thumbprint,
            not_after,
            permissions,
            collection_secondary_keys,
            collection_primary_key,
            public_key_pinning_hash,
        )
        self.certificate = certificate
        self.password = password

    def to_json(self) -> dict:
        json_dict = {
            "Name": self.name,
            "SecurityClearance": self.security_clearance,
            "Thumbprint": self.thumbprint,
            "Permissions": self.permissions,
            "CollectionSecondaryKeys": self.collection_secondary_keys,
            "CollectionPrimaryKey": self.collection_primary_key,
            "PublicKeyPinningHash": self.public_key_pinning_hash,
            "Certificate": self.certificate,
            "Password": self.password,
        }
        if self.not_after:
            json_dict.update({"NotAfter": Utils.datetime_to_string(self.not_after)})
        return json_dict

    @classmethod
    def from_json(cls, json_dict: dict) -> CertificateDefinition:
        return cls(
            json_dict["Certificate"],
            json_dict.get("Password", None),
            json_dict["Name"],
            json_dict["SecurityClearance"],
            json_dict["Thumbprint"],
            json_dict["NotAfter"],
            {item[0]: DatabaseAccess(item[1]) for item in json_dict["Permissions"].items()},
            json_dict["CollectionSecondaryKeys"],
            json_dict["CollectionPrimaryKey"],
            json_dict["PublicKeyPinningHash"],
        )


class CreateClientCertificateOperation(ServerOperation[CertificateRawData]):
    def __init__(
        self, name: str, permissions: Dict[str, DatabaseAccess], clearance: SecurityClearance, password: str = None
    ):
        if name is None:
            raise ValueError("name cannot be None")
        if permissions is None:
            raise ValueError("permissions cannot be None")

        super(CreateClientCertificateOperation, self).__init__()
        self.__name = name
        self.__permissions = permissions
        self.__clearance = clearance
        self.__password = password

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[CertificateRawData]:
        return self.__CreateClientCertificateCommand(self.__name, self.__permissions, self.__clearance, self.__password)

    class __CreateClientCertificateCommand(RavenCommand[CertificateRawData], RaftCommand):
        def __init__(self, name, permissions, clearance, password):
            if name is None:
                raise ValueError("name cannot be None")
            if permissions is None:
                raise ValueError("permissions cannot be None")

            super().__init__(CertificateRawData)

            self.__name = name
            self.__permissions = permissions
            self.__clearance = clearance
            self.__password = password

            self._response_type = RavenCommandResponseType.RAW

        def is_read_request(self) -> bool:
            return False

        def create_request(self, server_node):
            url = server_node.url + "/admin/certificates"
            data = {"Name": self.__name, "SecurityClearance": str(self.__clearance)}
            if self.__password:
                data["Password"] = self.__password

            permissions = {}
            for key, value in self.__permissions.items():
                permissions.update({key: str(value)})

            data["Permissions"] = permissions

            return requests.Request("POST", url, data=data)

        def set_response_raw(self, response: requests.Response, stream: bytes) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = CertificateRawData()

            try:
                self.result.raw_data = bytearray(stream)
            except Exception as e:
                self._throw_invalid_response(e)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetCertificatesResponse:
    def __init__(self, results: List[CertificateDefinition]):
        self.results = results

    @classmethod
    def from_json(cls, json_dict: dict) -> GetCertificatesResponse:
        results = []
        for cert_dict in json_dict["Results"]:
            results.append(CertificateDefinition.from_json(cert_dict))
        return cls(results)


class GetCertificateOperation(ServerOperation[CertificateDefinition]):
    def __init__(self, thumbprint: str):
        if thumbprint is None:
            raise ValueError("thumbprint cannot be None")

        self.__thumbprint = thumbprint

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[CertificateDefinition]:
        return self.__GetCertificateCommand(self.__thumbprint)

    class __GetCertificateCommand(RavenCommand[CertificateDefinition]):
        def __init__(self, thumbprint: str):
            if thumbprint is None:
                raise ValueError("thumbprint cannot be None")

            super().__init__(CertificateDefinition)
            self.__thumbprint = thumbprint

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/certificates?thumbprint={Utils.escape(self.__thumbprint, False,False)}"
            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return

            certificates = GetCertificatesResponse.from_json(json.loads(response))
            if len(certificates.results) != 1:
                self._throw_invalid_response()

            self.result = certificates.results[0]


class GetCertificatesOperation(ServerOperation[List[CertificateDefinition]]):
    def __init__(self, start: int, page_size: int):
        self.__start = start
        self.__page_size = page_size

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[CertificateDefinition]:
        return self.__GetCertificatesCommand(self.__start, self.__page_size)

    class __GetCertificatesCommand(RavenCommand[List[CertificateDefinition]]):
        def __init__(self, start: int, page_size: int):
            super().__init__(list)
            self.__start = start
            self.__page_size = page_size

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/certificates?start={self.__start}&pageSize={self.__page_size}"
            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return

            certificates = GetCertificatesResponse.from_json(json.loads(response))
            self.result = certificates.results


class GetCertificateMetadataOperation(ServerOperation[CertificateMetadata]):
    def __init__(self, thumbprint: str):
        if thumbprint is None:
            raise ValueError("thumbprint cannot be None")
        self.__thumbprint = thumbprint

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[CertificateMetadata]:
        return self.__GetCertificateMetadataCommand(self.__thumbprint)

    class __GetCertificateMetadataCommand(RavenCommand[CertificateMetadata]):
        def __init__(self, thumbprint: str):
            super().__init__(CertificateMetadata)
            self.__thumbprint = thumbprint

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            path = (
                f"{node.url}/admin/certificates"
                f"?thumbprint={Utils.escape(self.__thumbprint,True, False)}"
                f"&metadataOnly=true"
            )

            return requests.Request("GET", path)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return

            results = json.loads(response)["Results"]
            results = list(map(CertificateMetadata.from_json, results))

            if len(results) != 1:
                self._throw_invalid_response()

            self.result = results[0]


class GetCertificatesMetadataOperation(ServerOperation[List[CertificateMetadata]]):
    def __init__(self, name: str = None):
        self.__name = name

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[List[CertificateMetadata]]:
        return self.__GetCertificatesMetadataCommand(self.__name)

    class __GetCertificatesMetadataCommand(RavenCommand[List[CertificateMetadata]]):
        def __init__(self, name: str):
            super().__init__(list)
            self.__name = name

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            path = [node.url, "/admin/certificates?metadataOnly=true"]

            if self.__name:
                path.append("&name=")
                path.append(Utils.quote_key(self.__name))

            url = "".join(path)

            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = []
            for metadata in json.loads(response)["Results"]:
                self.result.append(CertificateMetadata.from_json(metadata))


class DeleteCertificateOperation(VoidServerOperation):
    def __init__(self, thumbprint: str):
        if thumbprint is None:
            raise ValueError("thumbprint cannot be None")

        super(DeleteCertificateOperation, self).__init__()
        self.__thumbprint = thumbprint

    def get_command(self, conventions: "DocumentConventions") -> VoidRavenCommand:
        return self.__DeleteCertificateCommand(self.__thumbprint)

    class __DeleteCertificateCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, thumbprint: str):
            if thumbprint is None:
                raise ValueError("certificate cannot be None")
            super().__init__()
            self.__thumbprint = thumbprint

        def create_request(self, server_node: ServerNode) -> requests.Request:
            url = f"{server_node.url}/admin/certificates?thumbprint={self.__thumbprint}"
            return requests.Request("DELETE", url)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class PutClientCertificateOperation(VoidServerOperation):
    def __init__(
        self, name: str, certificate: str, permissions: Dict[str, DatabaseAccess], clearance: SecurityClearance
    ):
        if certificate is None:
            raise ValueError("certificate cannot be None")

        if permissions is None:
            raise ValueError("permissions cannot be None")

        if name is None:
            raise ValueError("name cannot be None")

        self.__certificate = certificate
        self.__permissions = permissions
        self.__name = name
        self.__clearance = clearance

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.__PutClientCertificateCommand(self.__name, self.__certificate, self.__permissions, self.__clearance)

    class __PutClientCertificateCommand(VoidRavenCommand, RaftCommand):
        def __init__(
            self, name: str, certificate: str, permissions: Dict[str, DatabaseAccess], clearance: SecurityClearance
        ):
            if certificate is None:
                raise ValueError("certificate cannot be None")

            if permissions is None:
                raise ValueError("permissions cannot be None")

            super().__init__()
            self.__certificate = certificate
            self.__permissions = permissions
            self.__name = name
            self.__clearance = clearance

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/certificates"
            request = requests.Request("PUT", url)

            request.data = {
                "Name": self.__name,
                "Certificate": self.__certificate,
                "SecurityClearance": str(self.__clearance),
                "Permissions": self.__permissions,
            }

            return request

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class EditClientCertificateOperation(VoidServerOperation):
    class Parameters:
        def __init__(
            self, thumbprint: str, permissions: Dict[str, DatabaseAccess], name: str, clearance: SecurityClearance
        ):
            self.thumbprint = thumbprint
            self.permissions = permissions
            self.name = name
            self.clearance = clearance

    def __init__(self, parameters: Parameters):
        if parameters is None:
            raise ValueError("parameters cannot be None")

        if parameters.name is None:
            raise ValueError("name cannot be None")

        if parameters.thumbprint is None:
            raise ValueError("thumbprint cannot be None")

        if parameters.permissions is None:
            raise ValueError("permissions cannot be None")

        self.__name = parameters.name
        self.__thumbprint = parameters.thumbprint
        self.__permissions = parameters.permissions
        self.__clearance = parameters.clearance

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.__EditCertificateClientCommand(self.__thumbprint, self.__name, self.__permissions, self.__clearance)

    class __EditCertificateClientCommand(VoidRavenCommand, RaftCommand):
        def __init__(
            self, thumbprint: str, name: str, permissions: Dict[str, DatabaseAccess], clearance: SecurityClearance
        ):
            super().__init__()
            self.__thumbprint = thumbprint
            self.__name = name
            self.__permissions = permissions
            self.__clearance = clearance

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/certificates/edit"

            definition = CertificateDefinition()
            definition.thumbprint = self.__thumbprint
            definition.permissions = self.__permissions
            definition.security_clearance = self.__clearance
            definition.name = self.__name

            request = requests.Request("POST", url)
            request.data = definition.to_json()

            return request

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class ReplaceClusterCertificateOperation(VoidServerOperation):
    def __init__(self, cert_bytes: bytes, replace_immediately: bool):
        if cert_bytes is None:
            raise ValueError("cert_bytes cannot be None")

        self.__cert_bytes = cert_bytes
        self.__replace_immediately = replace_immediately

    @property
    def cert_bytes(self) -> bytes:
        return self.__cert_bytes

    @property
    def replace_immediately(self) -> bool:
        return self.__replace_immediately

    def get_command(self, conventions: "DocumentConventions") -> VoidRavenCommand:
        return self.__ReplaceClusterCertificateCommand(self.__cert_bytes, self.__replace_immediately)

    class __ReplaceClusterCertificateCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, cert_bytes: bytes, replace_immediately: bool):
            super().__init__()
            if cert_bytes is None:
                raise ValueError("cert_bytes cannot be None")

            self.__cert_bytes = cert_bytes
            self.__replace_immediately = replace_immediately

        @property
        def cert_bytes(self) -> bytes:
            return self.__cert_bytes

        @property
        def replace_immediately(self) -> bool:
            return self.__replace_immediately

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/admin/certificates/replace-cluster-cert"
                f"?replaceImmediately={'true' if self.__replace_immediately else 'false'}"
            )
            request = requests.Request("POST", url)
            request.data = json.loads(json.dumps({"Certificate": base64.b64encode(self.__cert_bytes).decode("utf-8")}))
            return request

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
