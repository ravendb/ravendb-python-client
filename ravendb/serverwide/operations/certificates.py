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


class SsoProvider(enum.Enum):
    GITHUB = "Github"
    GOOGLE = "Google"
    MICROSOFT = "Microsoft"
    WINDOWS = "Windows"

    def __str__(self):
        return self.value


class CertificateUsage(enum.Enum):
    RAVEN_SERVER = "RavenServer"
    RAVEN_SERVER_FOR_COMMUNICATION = "RavenServerForCommunication"
    CLIENT = "Client"
    SSO_SERVER = "SsoServer"
    SSO_CLIENT = "SsoClient"
    WELL_KNOWN_ISSUER = "WellKnownIssuer"

    def __str__(self):
        return self.value


class SsoIdentifier:
    """Identifies an SSO user with the provider that authenticated them."""

    def __init__(self, provider: SsoProvider = None, identifier: str = None, domain: str = None):
        self.provider = provider
        self.identifier = identifier
        self.domain = domain

    def to_json(self) -> dict:
        return {
            "Provider": self.provider.value if self.provider else None,
            "Domain": self.domain,
            "Identifier": self.identifier,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> SsoIdentifier:
        provider = json_dict.get("Provider")
        return cls(
            provider=SsoProvider(provider) if provider else None,
            identifier=json_dict.get("Identifier"),
            domain=json_dict.get("Domain"),
        )


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
        not_before: datetime = None,
        disabled: bool = False,
        usage: CertificateUsage = None,
        sso_server_public_key_pinning_hashes: List[str] = None,
        allow_any_sso_server: bool = False,
        sso_identifiers: List[SsoIdentifier] = None,
    ):
        self.name = name
        self.security_clearance = security_clearance
        self.thumbprint = thumbprint
        self.not_after = not_after
        self.permissions = permissions
        self.collection_primary_key = collection_primary_key
        self.collection_secondary_keys = collection_secondary_keys
        self.public_key_pinning_hash = public_key_pinning_hash
        self.not_before = not_before
        self.disabled = disabled
        self.usage = usage
        # The SSO servers allowed to authenticate this user, by public-key pinning hash.
        self.sso_server_public_key_pinning_hashes = sso_server_public_key_pinning_hashes or []
        self.allow_any_sso_server = allow_any_sso_server
        self.sso_identifiers = sso_identifiers or []

    @staticmethod
    def _sso_fields_from_json(json_dict: dict) -> dict:
        usage = json_dict.get("Usage")
        sso_identifiers = json_dict.get("SsoIdentifiers")
        return {
            "usage": CertificateUsage(usage) if usage else None,
            "sso_server_public_key_pinning_hashes": json_dict.get("SsoServerPublicKeyPinningHashes"),
            "allow_any_sso_server": json_dict.get("AllowAnySsoServer", False),
            "sso_identifiers": (
                [SsoIdentifier.from_json(identifier) for identifier in sso_identifiers] if sso_identifiers else None
            ),
        }

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
            Utils.string_to_datetime(json_dict["NotBefore"]) if "NotBefore" in json_dict else None,
            json_dict.get("Disabled", False),
            **cls._sso_fields_from_json(json_dict),
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
        disabled: bool = False,
        usage: CertificateUsage = None,
        sso_server_public_key_pinning_hashes: List[str] = None,
        allow_any_sso_server: bool = False,
        sso_identifiers: List[SsoIdentifier] = None,
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
            disabled=disabled,
            usage=usage,
            sso_server_public_key_pinning_hashes=sso_server_public_key_pinning_hashes,
            allow_any_sso_server=allow_any_sso_server,
            sso_identifiers=sso_identifiers,
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
            "Disabled": self.disabled,
            "Usage": self.usage.value if self.usage else None,
            "SsoServerPublicKeyPinningHashes": self.sso_server_public_key_pinning_hashes,
            "AllowAnySsoServer": self.allow_any_sso_server,
            "SsoIdentifiers": [identifier.to_json() for identifier in self.sso_identifiers],
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
            disabled=json_dict.get("Disabled", False),
            **cls._sso_fields_from_json(json_dict),
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
            self,
            thumbprint: str,
            permissions: Dict[str, DatabaseAccess],
            name: str,
            clearance: SecurityClearance,
            disabled: bool = False,
            sso_server_public_key_pinning_hashes: List[str] = None,
            allow_any_sso_server: bool = None,
            sso_identifiers: List[SsoIdentifier] = None,
        ):
            self.thumbprint = thumbprint
            self.permissions = permissions
            self.name = name
            self.clearance = clearance
            self.disabled = disabled
            # SSO configuration is opt-in: leave these None to keep the stored settings
            # untouched, which is what a plain certificate edit or a disabled-only toggle
            # wants. Setting any of them - even to an empty list - replaces the stored value,
            # which is how an SSO user's authorizing servers or identifiers get cleared.
            self.sso_server_public_key_pinning_hashes = sso_server_public_key_pinning_hashes
            self.allow_any_sso_server = allow_any_sso_server
            self.sso_identifiers = sso_identifiers

    def __init__(self, parameters: Parameters):
        if parameters is None:
            raise ValueError("parameters cannot be None")

        if parameters.name is None:
            raise ValueError("name cannot be None")

        if parameters.thumbprint is None:
            raise ValueError("thumbprint cannot be None")

        if parameters.permissions is None:
            raise ValueError("permissions cannot be None")

        self.__parameters = parameters

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.__EditCertificateClientCommand(self.__parameters)

    class __EditCertificateClientCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, parameters: "EditClientCertificateOperation.Parameters"):
            super().__init__()
            self.__parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/certificates/edit"

            parameters = self.__parameters
            definition = {
                "Thumbprint": parameters.thumbprint,
                "Name": parameters.name,
                "SecurityClearance": parameters.clearance,
                "Disabled": parameters.disabled,
                "Permissions": parameters.permissions,
            }

            # Written only when explicitly provided, so the server leaves the existing SSO
            # configuration alone on a partial edit and clears it when an empty list is sent.
            if parameters.sso_server_public_key_pinning_hashes is not None:
                definition["SsoServerPublicKeyPinningHashes"] = parameters.sso_server_public_key_pinning_hashes
            if parameters.allow_any_sso_server is not None:
                definition["AllowAnySsoServer"] = parameters.allow_any_sso_server
            if parameters.sso_identifiers is not None:
                definition["SsoIdentifiers"] = [identifier.to_json() for identifier in parameters.sso_identifiers]

            request = requests.Request("POST", url)
            request.data = definition

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
