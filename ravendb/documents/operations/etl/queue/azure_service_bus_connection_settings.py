from __future__ import annotations

from typing import Any, Dict, Optional

from ravendb.exceptions.exceptions import InvalidOperationException


class AzureServiceBusEntraId:
    def __init__(
        self,
        namespace: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        self.namespace = namespace
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def is_valid(self) -> bool:
        return (
            not _is_blank(self.namespace)
            and not _is_blank(self.tenant_id)
            and not _is_blank(self.client_id)
            and not _is_blank(self.client_secret)
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Namespace": self.namespace,
            "TenantId": self.tenant_id,
            "ClientId": self.client_id,
            "ClientSecret": self.client_secret,
        }

    def to_audit_json(self) -> Dict[str, Any]:
        # ClientSecret is masked in audit output.
        return {
            "Namespace": self.namespace,
            "TenantId": self.tenant_id,
            "ClientId": self.client_id,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> AzureServiceBusEntraId:
        return cls(
            namespace=json_dict.get("Namespace"),
            tenant_id=json_dict.get("TenantId"),
            client_id=json_dict.get("ClientId"),
            client_secret=json_dict.get("ClientSecret"),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureServiceBusEntraId):
            return False
        return (
            self.namespace == other.namespace
            and self.tenant_id == other.tenant_id
            and self.client_id == other.client_id
            and self.client_secret == other.client_secret
        )

    def __hash__(self) -> int:
        return hash((self.namespace, self.tenant_id, self.client_id, self.client_secret))


class AzureServiceBusPasswordless:
    """Machine authentication (Managed Identity); only a namespace is required."""

    def __init__(self, namespace: Optional[str] = None):
        self.namespace = namespace

    def is_valid(self) -> bool:
        return not _is_blank(self.namespace)

    def to_json(self) -> Dict[str, Any]:
        return {"Namespace": self.namespace}

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> AzureServiceBusPasswordless:
        return cls(namespace=json_dict.get("Namespace"))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureServiceBusPasswordless):
            return False
        return self.namespace == other.namespace

    def __hash__(self) -> int:
        return hash(self.namespace)


def _is_blank(value: Optional[str]) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


class AzureServiceBusConnectionSettings:
    def __init__(
        self,
        connection_string: Optional[str] = None,
        entra_id: Optional[AzureServiceBusEntraId] = None,
        passwordless: Optional[AzureServiceBusPasswordless] = None,
    ):
        self.connection_string = connection_string
        self.entra_id = entra_id
        self.passwordless = passwordless

    def is_valid_connection(self) -> bool:
        if not self._is_only_one_connection_provided():
            return False

        if self.entra_id is not None and self.entra_id.is_valid():
            return True

        if self.passwordless is not None and self.passwordless.is_valid():
            return True

        return self._try_extract_endpoint() is not None

    def _is_only_one_connection_provided(self) -> bool:
        count = 0
        if self.entra_id is not None:
            count += 1
        if not _is_blank(self.connection_string):
            count += 1
        if self.passwordless is not None:
            count += 1
        return count == 1

    def get_service_bus_url(self) -> str:
        if not _is_blank(self.connection_string):
            endpoint = self._try_extract_endpoint()
            if endpoint is not None:
                return endpoint
            raise InvalidOperationException("No endpoint provided")

        return f"sb://{self._get_namespace()}/"

    def _try_extract_endpoint(self) -> Optional[str]:
        if not self.connection_string:
            return None

        # Case-insensitive search, matching IndexOf(OrdinalIgnoreCase) in the C# client.
        lowered = self.connection_string.lower()
        start = lowered.find("sb://")
        if start < 0:
            return None

        end = self.connection_string.find(";", start)
        if end < 0:
            return self.connection_string[start:]

        return self.connection_string[start:end]

    def _get_namespace(self) -> str:
        if self.entra_id is not None:
            return self.entra_id.namespace

        if self.passwordless is not None:
            return self.passwordless.namespace

        raise InvalidOperationException("No namespace provided")

    def to_json(self) -> Dict[str, Any]:
        # Only set auth fields are written. The connection string is written when it is
        # not None and not "" (IsNullOrEmpty), so a whitespace-only value IS written even
        # though the exactly-one rule treats it as unset.
        json_dict = {}
        if self.connection_string:
            json_dict["ConnectionString"] = self.connection_string
        if self.entra_id is not None:
            json_dict["EntraId"] = self.entra_id.to_json()
        if self.passwordless is not None:
            json_dict["Passwordless"] = self.passwordless.to_json()
        return json_dict

    def to_audit_json(self) -> Dict[str, Any]:
        json_dict = {}
        if self.connection_string:
            json_dict["ConnectionString"] = "<Contains-Secrets>"
        if self.entra_id is not None:
            json_dict["EntraId"] = self.entra_id.to_audit_json()
        if self.passwordless is not None:
            json_dict["Passwordless"] = self.passwordless.to_json()
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> AzureServiceBusConnectionSettings:
        entra_id = json_dict.get("EntraId")
        passwordless = json_dict.get("Passwordless")
        return cls(
            connection_string=json_dict.get("ConnectionString"),
            entra_id=AzureServiceBusEntraId.from_json(entra_id) if entra_id else None,
            passwordless=AzureServiceBusPasswordless.from_json(passwordless) if passwordless else None,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureServiceBusConnectionSettings):
            return False
        return (
            self.connection_string == other.connection_string
            and self.entra_id == other.entra_id
            and self.passwordless == other.passwordless
        )

    def __hash__(self) -> int:
        return hash((self.connection_string, self.entra_id, self.passwordless))
