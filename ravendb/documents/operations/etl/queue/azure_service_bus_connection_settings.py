from typing import Optional, Dict, Any


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

    def to_json(self) -> Dict[str, Any]:
        return {
            "Namespace": self.namespace,
            "TenantId": self.tenant_id,
            "ClientId": self.client_id,
            "ClientSecret": self.client_secret,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        return cls(
            namespace=json_dict.get("Namespace"),
            tenant_id=json_dict.get("TenantId"),
            client_id=json_dict.get("ClientId"),
            client_secret=json_dict.get("ClientSecret"),
        )


class AzureServiceBusPasswordless:
    def __init__(self, namespace: Optional[str] = None):
        self.namespace = namespace

    def to_json(self) -> Dict[str, Any]:
        return {
            "Namespace": self.namespace,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        return cls(
            namespace=json_dict.get("Namespace"),
        )


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

    def to_json(self) -> Dict[str, Any]:
        json_dict = {}
        if self.connection_string:
            json_dict["ConnectionString"] = self.connection_string
        if self.entra_id:
            json_dict["EntraId"] = self.entra_id.to_json()
        if self.passwordless:
            json_dict["Passwordless"] = self.passwordless.to_json()
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        entra_id_dict = json_dict.get("EntraId")
        passwordless_dict = json_dict.get("Passwordless")
        return cls(
            connection_string=json_dict.get("ConnectionString"),
            entra_id=AzureServiceBusEntraId.from_json(entra_id_dict) if entra_id_dict else None,
            passwordless=AzureServiceBusPasswordless.from_json(passwordless_dict) if passwordless_dict else None,
        )
