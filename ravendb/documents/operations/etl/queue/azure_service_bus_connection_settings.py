from typing import Optional, Dict, Any


class AzureServiceBusEntraId:
    """Microsoft Entra ID (service principal) credentials for an Azure Service Bus namespace."""

    def __init__(
        self,
        namespace: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        # Fully qualified namespace, e.g. 'mynamespace.servicebus.windows.net'.
        self.namespace = namespace
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def to_json(self) -> Dict[str, Any]:
        return {
            "Namespace": self.namespace,
            "TenantId": self.tenant_id,
            "ClientId": self.client_id,
            "ClientSecret": self.client_secret,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AzureServiceBusEntraId":
        return cls(
            namespace=json_dict.get("Namespace"),
            tenant_id=json_dict.get("TenantId"),
            client_id=json_dict.get("ClientId"),
            client_secret=json_dict.get("ClientSecret"),
        )


class AzureServiceBusPasswordless:
    """Machine authentication (Managed Identity) against an Azure Service Bus namespace."""

    def __init__(self, namespace: Optional[str] = None):
        # Fully qualified namespace, e.g. 'mynamespace.servicebus.windows.net'.
        self.namespace = namespace

    def to_json(self) -> Dict[str, Any]:
        return {"Namespace": self.namespace}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AzureServiceBusPasswordless":
        return cls(namespace=json_dict.get("Namespace"))


class AzureServiceBusConnectionSettings:
    """
    Azure Service Bus connection settings. Exactly one authentication method may be set:
    a connection string, Entra ID credentials, or passwordless (Managed Identity).
    The server rejects a configuration that sets none or more than one.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        entra_id: Optional[AzureServiceBusEntraId] = None,
        passwordless: Optional[AzureServiceBusPasswordless] = None,
    ):
        self.connection_string = connection_string
        self.entra_id = entra_id
        self.passwordless = passwordless

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConnectionString": self.connection_string,
            "EntraId": self.entra_id.to_json() if self.entra_id else None,
            "Passwordless": self.passwordless.to_json() if self.passwordless else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AzureServiceBusConnectionSettings":
        entra_id_dict = json_dict.get("EntraId")
        passwordless_dict = json_dict.get("Passwordless")
        return cls(
            connection_string=json_dict.get("ConnectionString"),
            entra_id=AzureServiceBusEntraId.from_json(entra_id_dict) if entra_id_dict else None,
            passwordless=AzureServiceBusPasswordless.from_json(passwordless_dict) if passwordless_dict else None,
        )
