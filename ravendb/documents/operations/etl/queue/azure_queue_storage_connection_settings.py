from typing import Optional, Dict, Any


class EntraId:
    def __init__(
        self,
        storage_account_name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        self.storage_account_name = storage_account_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def to_json(self) -> Dict[str, Any]:
        return {
            "StorageAccountName": self.storage_account_name,
            "TenantId": self.tenant_id,
            "ClientId": self.client_id,
            "ClientSecret": self.client_secret,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        return cls(
            storage_account_name=json_dict.get("StorageAccountName"),
            tenant_id=json_dict.get("TenantId"),
            client_id=json_dict.get("ClientId"),
            client_secret=json_dict.get("ClientSecret"),
        )


class Passwordless:
    def __init__(self, storage_account_name: Optional[str] = None):
        self.storage_account_name = storage_account_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "StorageAccountName": self.storage_account_name,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        return cls(
            storage_account_name=json_dict.get("StorageAccountName"),
        )


class AzureQueueStorageConnectionSettings:
    def __init__(
        self,
        entra_id: Optional[EntraId] = None,
        connection_string: Optional[str] = None,
        passwordless: Optional[Passwordless] = None,
    ):
        self.entra_id = entra_id
        self.connection_string = connection_string
        self.passwordless = passwordless

    def to_json(self) -> Dict[str, Any]:
        return {
            "EntraId": self.entra_id.to_json() if self.entra_id else None,
            "ConnectionString": self.connection_string,
            "Passwordless": self.passwordless.to_json() if self.passwordless else None,
        }

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        entra_id_dict = json_dict.get("EntraId")
        passwordless_dict = json_dict.get("Passwordless")
        return cls(
            entra_id=EntraId.from_json(entra_id_dict) if entra_id_dict else None,
            connection_string=json_dict.get("ConnectionString"),
            passwordless=Passwordless.from_json(passwordless_dict) if passwordless_dict else None,
        )
