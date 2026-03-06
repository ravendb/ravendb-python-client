from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, Dict, Any, List

import requests

from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import MaintenanceOperation, OperationIdResult
from ravendb.util.util import RaftIdGenerator


class SchemaDefinition:
    def __init__(
        self,
        schema: str = None,
        disabled: bool = False,
        last_modified_time: Optional[datetime] = None,
    ):
        self.schema = schema
        self.disabled = disabled
        self.last_modified_time = last_modified_time or datetime.utcnow()

    def to_json(self) -> Dict[str, Any]:
        return {
            "Schema": self.schema,
            "Disabled": self.disabled,
            "LastModifiedTime": self.last_modified_time.isoformat(),
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "SchemaDefinition":
        last_modified_raw = json_dict.get("LastModifiedTime")
        last_modified = datetime.fromisoformat(last_modified_raw) if last_modified_raw else None
        return cls(
            schema=json_dict.get("Schema"),
            disabled=json_dict.get("Disabled", False),
            last_modified_time=last_modified,
        )


class SchemaValidationConfiguration:
    def __init__(
        self,
        disabled: bool = False,
        validators_per_collection: Optional[Dict[str, SchemaDefinition]] = None,
    ):
        self.disabled = disabled
        self.validators_per_collection: Dict[str, SchemaDefinition] = validators_per_collection or {}

    def to_json(self) -> Dict[str, Any]:
        validators = {k: v.to_json() for k, v in self.validators_per_collection.items()}
        return {
            "Disabled": self.disabled,
            "ValidatorsPerCollection": validators,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "SchemaValidationConfiguration":
        raw_validators = json_dict.get("ValidatorsPerCollection") or {}
        validators = {k: SchemaDefinition.from_json(v) for k, v in raw_validators.items()}
        return cls(
            disabled=json_dict.get("Disabled", False),
            validators_per_collection=validators,
        )


class ConfigureSchemaValidationOperationResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ConfigureSchemaValidationOperationResult":
        return cls(raft_command_index=json_dict.get("RaftCommandIndex"))


class ConfigureSchemaValidationOperation(MaintenanceOperation[ConfigureSchemaValidationOperationResult]):
    def __init__(self, configuration: SchemaValidationConfiguration):
        if configuration is None:
            raise ValueError("configuration cannot be None")
        self._configuration = configuration

    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self._ConfigureSchemaValidationCommand(self._configuration)

    class _ConfigureSchemaValidationCommand(RavenCommand[ConfigureSchemaValidationOperationResult], RaftCommand):
        def __init__(self, configuration: SchemaValidationConfiguration):
            super().__init__(ConfigureSchemaValidationOperationResult)
            self._configuration = configuration
            self._raft_unique_request_id = RaftIdGenerator.new_id()

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/schema-validation/config"
            request = requests.Request("POST", url)
            request.data = json.dumps(self._configuration.to_json())
            request.headers = {"Content-Type": "application/json"}
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ConfigureSchemaValidationOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return self._raft_unique_request_id


class GetSchemaValidationConfiguration(MaintenanceOperation[Optional[SchemaValidationConfiguration]]):
    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self._GetSchemaValidationCommand()

    class _GetSchemaValidationCommand(RavenCommand[Optional[SchemaValidationConfiguration]]):
        def __init__(self):
            super().__init__(SchemaValidationConfiguration)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/schema-validation/config"
            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self.result = None
                return
            self.result = SchemaValidationConfiguration.from_json(json.loads(response))


class ValidateSchemaProgress:
    def __init__(self, error_count: int = 0, validated_count: int = 0):
        self.error_count = error_count
        self.validated_count = validated_count

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ValidateSchemaProgress":
        return cls(
            error_count=json_dict.get("ErrorCount", 0),
            validated_count=json_dict.get("ValidatedCount", 0),
        )


class ValidateSchemaResult(ValidateSchemaProgress):
    def __init__(
        self,
        error_count: int = 0,
        validated_count: int = 0,
        errors: Optional[Dict[str, str]] = None,
        last_etag: int = 0,
    ):
        super().__init__(error_count=error_count, validated_count=validated_count)
        self.errors: Dict[str, str] = errors or {}
        self.last_etag = last_etag

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ValidateSchemaResult":
        return cls(
            error_count=json_dict.get("ErrorCount", 0),
            validated_count=json_dict.get("ValidatedCount", 0),
            errors=json_dict.get("Errors") or {},
            last_etag=json_dict.get("LastEtag", 0),
        )


class StartSchemaValidationOperation(MaintenanceOperation[OperationIdResult]):
    class Parameters:
        def __init__(
            self,
            schema_definition: str,
            collection: str,
            max_error_messages: Optional[int] = None,
            max_documents_to_validate: Optional[int] = None,
            start_etag: Optional[int] = None,
        ):
            self.schema_definition = schema_definition
            self.collection = collection
            self.max_error_messages = max_error_messages
            self.max_documents_to_validate = max_documents_to_validate
            self.start_etag = start_etag

        def to_json(self) -> Dict[str, Any]:
            d: Dict[str, Any] = {
                "SchemaDefinition": self.schema_definition,
                "Collection": self.collection,
            }
            if self.max_error_messages is not None:
                d["MaxErrorMessages"] = self.max_error_messages
            if self.max_documents_to_validate is not None:
                d["MaxDocumentsToValidate"] = self.max_documents_to_validate
            if self.start_etag is not None:
                d["StartEtag"] = self.start_etag
            return d

    def __init__(self, parameters: "StartSchemaValidationOperation.Parameters"):
        if parameters is None:
            raise ValueError("parameters cannot be None")
        if not parameters.schema_definition or not parameters.schema_definition.strip():
            raise ValueError("Schema must be provided.")
        if not parameters.collection or not parameters.collection.strip():
            raise ValueError("Collection must be provided.")
        if parameters.max_error_messages is not None and parameters.max_error_messages < 0:
            raise ValueError("max_error_messages must be >= 0.")
        if parameters.max_documents_to_validate is not None and parameters.max_documents_to_validate <= 0:
            raise ValueError("max_documents_to_validate must be > 0.")
        self._parameters = parameters

    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self._StartSchemaValidationCommand(self._parameters)

    class _StartSchemaValidationCommand(RavenCommand[OperationIdResult], RaftCommand):
        def __init__(self, parameters: "StartSchemaValidationOperation.Parameters"):
            super().__init__(OperationIdResult)
            self._parameters = parameters
            self._raft_unique_request_id = RaftIdGenerator.new_id()

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/schema-validation/validate"
            request = requests.Request("POST", url)
            request.data = json.dumps(self._parameters.to_json())
            request.headers = {"Content-Type": "application/json"}
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = OperationIdResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return self._raft_unique_request_id
