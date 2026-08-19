from __future__ import annotations

import enum
import http
import json
import urllib.parse
from datetime import datetime
from typing import Optional, TYPE_CHECKING, List, Dict

import requests

from ravendb.primitives import constants
from ravendb.data.operation import AttachmentType
from ravendb.documents.operations.backups.settings import S3StorageClass
from ravendb.documents.operations.definitions import IOperation, VoidOperation, MaintenanceOperation
from ravendb.http.http_cache import HttpCache
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType, VoidRavenCommand
from ravendb.http.topology import RaftCommand
from ravendb.util.util import RaftIdGenerator
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents import DocumentStore
    from ravendb.documents.conventions import DocumentConventions


class AttachmentName:
    def __init__(self, name: str, hash: str, content_type: str, size: int):
        self.name = name
        self.hash = hash
        self.content_type = content_type
        self.size = size
        self.remote_parameters: Optional[RemoteAttachmentParameters] = None

    @classmethod
    def from_json(cls, json_dict: dict) -> AttachmentName:
        obj = cls(json_dict["Name"], json_dict["Hash"], json_dict["ContentType"], json_dict["Size"])
        remote_raw = json_dict.get("RemoteParameters")
        if remote_raw is not None:
            obj.remote_parameters = RemoteAttachmentParameters.from_json(remote_raw)
        return obj


class AttachmentDetails(AttachmentName):
    def __init__(
        self, name: str, hash: str, content_type: str, size: int, change_vector: str = None, document_id: str = None
    ):
        super().__init__(name, hash, content_type, size)
        self.change_vector = change_vector
        self.document_id = document_id

    @classmethod
    def from_json(cls, json_dict: dict) -> AttachmentDetails:
        return cls(
            json_dict["Name"],
            json_dict["Hash"],
            json_dict["ContentType"],
            json_dict["Size"],
            json_dict["ChangeVector"],
            json_dict["DocumentId"],
        )


class CloseableAttachmentResult:
    def __init__(self, response: requests.Response, details: AttachmentDetails):
        self.__details = details
        self.__response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def data(self):
        return self.__response.content

    @property
    def details(self):
        return self.__details

    def close(self):
        self.__response.close()


class AttachmentRequest:
    def __init__(self, document_id: str, name: str):
        if document_id.isspace():
            raise ValueError("Document_id cannot be None or whitespace")
        if name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        self.document_id = document_id
        self.name = name


class StoreAttachmentParameters:
    def __init__(self, name: str, stream, content_type: Optional[str] = None, change_vector: Optional[str] = None):
        if not name or name.isspace():
            raise ValueError("Attachment name cannot be null or whitespace.")
        if stream is None:
            raise ValueError("Attachment stream cannot be null.")
        self.name = name
        self.stream = stream
        self.change_vector = change_vector
        self.content_type = content_type
        self.remote_parameters: Optional[RemoteAttachmentParameters] = None


class PutAttachmentOperation(IOperation[AttachmentDetails]):
    def __init__(
        self,
        document_id: str,
        name: str,
        stream: bytes,
        content_type: Optional[str] = None,
        remote_parameters: Optional[RemoteAttachmentParameters] = None,
        change_vector: Optional[str] = None,
    ):
        super().__init__()
        self.__document_id = document_id
        self.__name = name
        self.__stream = stream
        self.__content_type = content_type
        self.__remote_parameters = remote_parameters
        self.__change_vector = change_vector

    @classmethod
    def from_store_attachment_parameters(
        cls, document_id: str, parameters: StoreAttachmentParameters
    ) -> PutAttachmentOperation:
        return cls(
            document_id,
            parameters.name,
            parameters.stream,
            parameters.content_type,
            parameters.remote_parameters,
            parameters.change_vector,
        )

    def get_command(self, store, conventions, cache=None):
        return self.__PutAttachmentCommand(
            self.__document_id,
            self.__name,
            self.__stream,
            self.__content_type,
            self.__change_vector,
            self.__remote_parameters,
        )

    class __PutAttachmentCommand(RavenCommand[AttachmentDetails]):
        def __init__(
            self,
            document_id: str,
            name: str,
            stream: bytes,
            content_type: str,
            change_vector: str,
            remote_parameters: Optional[RemoteAttachmentParameters] = None,
        ):
            super().__init__(AttachmentDetails)

            if not document_id:
                raise ValueError("document_id cannot be None or blank")

            if not name:
                raise ValueError("name cannot be None or blank")

            self.__document_id = document_id
            self.__name = name
            self.__stream = stream
            self.__content_type = content_type
            self.__change_vector = change_vector
            self.__remote_parameters = remote_parameters

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/attachments?id={self.__document_id}"  # todo : escape
                f"&name={Utils.escape(self.__name, True,False)}"
            )

            if not self.__content_type.isspace():
                url += f"&contentType={Utils.escape(self.__content_type, True, False)}"

            if self.__remote_parameters is not None:
                url += f"&remoteAt={Utils.escape(Utils.datetime_to_string(self.__remote_parameters.at), True, False)}"
                url += f"&remoteIdentifier={Utils.escape(self.__remote_parameters.identifier, True, False)}"

            request = requests.Request("PUT", url)
            if isinstance(self.__stream, (bytes, bytearray)):
                request.files = {self.__name: (self.__name, self.__stream, self.__content_type)}
            else:
                request.data = self.__stream
            self._add_change_vector_if_not_none(self.__change_vector, request)
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = AttachmentDetails.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False


class GetAttachmentOperation(IOperation):
    def __init__(self, document_id: str, name: str, attachment_type: AttachmentType, change_vector: Optional[str]):
        if document_id is None:
            raise ValueError("Invalid document_id")
        if name is None:
            raise ValueError("Invalid name")

        if attachment_type != AttachmentType.document and change_vector is None:
            raise ValueError(
                "change_vector",
                "Change Vector cannot be null for attachment type {0}".format(attachment_type),
            )

        super(GetAttachmentOperation, self).__init__()
        self.__document_id = document_id
        self.__name = name
        self.__attachment_type = attachment_type
        self.__change_vector = change_vector

    def get_command(self, store, conventions, cache=None) -> RavenCommand[dict]:
        return self.__GetAttachmentCommand(
            self.__document_id, self.__name, self.__attachment_type, self.__change_vector
        )

    class __GetAttachmentCommand(RavenCommand[CloseableAttachmentResult]):
        def __init__(self, document_id: str, name: str, attachment_type: AttachmentType, change_vector: str):
            super().__init__(CloseableAttachmentResult)
            self.__document_id = document_id
            self.__name = name
            self.__attachment_type = attachment_type
            self.__change_vector = change_vector

        def create_request(self, server_node: ServerNode) -> requests.Request:
            method = "GET"
            url = (
                f"{server_node.url}/databases/{server_node.database}/attachments"
                f"?id={Utils.quote_key(self.__document_id)}&name={Utils.quote_key(self.__name)}"
            )
            data = None
            if self.__attachment_type != AttachmentType.document:
                method = "POST"
                data = {
                    "Type": str(self.__attachment_type),
                    "ChangeVector": self.__change_vector,
                }
            return requests.Request(method, url, data=data)

        def is_read_request(self) -> bool:
            return True

        def process_response(self, cache: HttpCache, response: requests.Response, url) -> http.ResponseDisposeHandling:
            content_type = response.headers.get("Content-Type")
            change_vector = response.headers.get(constants.Headers.ETAG)
            hash = response.headers.get(constants.Headers.ATTACHMENT_HASH)
            size = response.headers.get(constants.Headers.ATTACHMENT_SIZE, 0)

            remote_identifier_raw = response.headers.get(constants.Headers.ATTACHMENT_REMOTE_PARAMETERS_IDENTIFIER)
            remote_identifier = urllib.parse.unquote(remote_identifier_raw) if remote_identifier_raw else None
            remote_parameters = None
            if remote_identifier:
                at_raw = response.headers.get(constants.Headers.ATTACHMENT_REMOTE_PARAMETERS_AT)
                if at_raw is None:
                    raise RuntimeError(
                        f"Attachment remote parameters header '{constants.Headers.ATTACHMENT_REMOTE_PARAMETERS_AT}' "
                        f"is missing for attachment '{self.__name}' on document '{self.__document_id}'."
                    )
                flags_raw = response.headers.get(constants.Headers.ATTACHMENT_REMOTE_PARAMETERS_FLAGS)
                if flags_raw is None:
                    raise RuntimeError(
                        f"Attachment remote parameters header '{constants.Headers.ATTACHMENT_REMOTE_PARAMETERS_FLAGS}' "
                        f"is missing for attachment '{self.__name}' on document '{self.__document_id}'."
                    )
                remote_parameters = RemoteAttachmentParameters(remote_identifier, Utils.string_to_datetime(at_raw))
                remote_parameters.flags = RemoteAttachmentFlags.from_str(flags_raw)

            attachment_details = AttachmentDetails(
                self.__name, hash, content_type, size, change_vector, self.__document_id
            )
            attachment_details.remote_parameters = remote_parameters
            self.result = CloseableAttachmentResult(response, attachment_details)
            return ResponseDisposeHandling.MANUALLY


class GetAttachmentsOperation(IOperation[CloseableAttachmentResult]):  # unfinished
    def __init__(self, attachments: List[AttachmentRequest], attachment_type: AttachmentType):
        self.__attachment_type = attachment_type
        self.__attachments = attachments

    def get_command(
        self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache
    ) -> RavenCommand[CloseableAttachmentResult]:
        return self.__GetAttachmentsCommand(self.__attachments, self.__attachment_type)

    class __GetAttachmentsCommand(RavenCommand[CloseableAttachmentResult]):
        def __init__(self, attachments: List[AttachmentRequest], attachment_type: AttachmentType):
            super().__init__(CloseableAttachmentResult)
            self.__attachment_type = attachment_type
            self.__attachments = attachments
            self._response_type = RavenCommandResponseType.EMPTY
            self.__attachments_metadata = []

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "POST",
                f"{node.url}/databases/{node.database}/attachments/bulk",
                data={
                    "AttachmentType": str(self.__attachment_type),
                    "Attachments": [
                        {"DocumentId": attachment.document_id, "Name": attachment.name}
                        for attachment in self.__attachments
                    ],
                },
            )

        def is_read_request(self) -> bool:
            return True

        def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
            raise NotImplementedError("Fetching multiple attachments is yet to be implemented")
            # todo: we can't decode it - maybe split bytes (part 1 is json, second part is binary attachments data) -


class DeleteAttachmentOperation(VoidOperation):
    def __init__(self, document_id: str, name: str, change_vector: Optional[str] = None):
        self._document_id = document_id
        self._name = name
        self._change_vector = change_vector

    def get_command(self, store: "DocumentStore", conventions: "DocumentConventions", cache=None) -> VoidRavenCommand:
        return self.__DeleteAttachmentCommand(self._document_id, self._name, self._change_vector)

    class __DeleteAttachmentCommand(VoidRavenCommand):
        def __init__(self, document_id: str, name: str, change_vector: Optional[str] = None):
            super().__init__()
            if not document_id:
                raise ValueError("Invalid document_id")
            if not name:
                raise ValueError("Invalid name")

            self.__document_id = document_id
            self.__name = name
            self.__change_vector = change_vector

        def create_request(self, server_node):
            request = requests.Request(
                "DELETE",
                f"{server_node.url}/databases/{server_node.database}/attachments?id={Utils.quote_key(self.__document_id)}&name={Utils.quote_key(self.__name)}",
            )
            self._add_change_vector_if_not_none(self.__change_vector, request)
            return request


class DeleteAttachmentsOperation(VoidOperation):
    def __init__(self, attachments: List[AttachmentRequest]):
        self.__attachments = attachments

    def get_command(self, store: "DocumentStore", conventions: "DocumentConventions", cache=None) -> VoidRavenCommand:
        return self.__DeleteAttachmentsCommand(self.__attachments)

    class __DeleteAttachmentsCommand(VoidRavenCommand):
        def __init__(self, attachments: List[AttachmentRequest]):
            super().__init__()
            if attachments is None:
                raise ValueError("Attachments cannot be None")
            self.__attachments = attachments

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "DELETE",
                f"{node.url}/databases/{node.database}/attachments/bulk",
                data={"Attachments": [{"DocumentId": a.document_id, "Name": a.name} for a in self.__attachments]},
            )


class RemoteAttachmentFlags(enum.IntFlag):
    NONE = 0
    REMOTE = 0x1

    def to_str(self) -> str:
        """Returns the PascalCase string representation matching C# Flags.ToString()."""
        return self.name.capitalize() if self != RemoteAttachmentFlags.NONE else "None"

    @classmethod
    def from_str(cls, value: str) -> RemoteAttachmentFlags:
        """Parses a PascalCase string from the server (e.g. 'None', 'Remote')."""
        return cls[value.upper()]


class RemoteAttachmentsS3Settings:
    def __init__(
        self,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        remote_folder_name: str = None,
        bucket_name: str = None,
        custom_server_url: str = None,
        force_path_style: bool = None,
        storage_class: Optional[S3StorageClass] = None,
        disable_checksum_validation: bool = False,
    ):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_region_name = aws_region_name
        self.remote_folder_name = remote_folder_name
        self.bucket_name = bucket_name
        self.custom_server_url = custom_server_url
        self.force_path_style = force_path_style
        self.storage_class = storage_class
        self.disable_checksum_validation = disable_checksum_validation

    @classmethod
    def from_json(cls, json_dict: dict) -> RemoteAttachmentsS3Settings:
        storage_class_raw = json_dict.get("StorageClass")
        return cls(
            json_dict.get("AwsAccessKey"),
            json_dict.get("AwsSecretKey"),
            json_dict.get("AwsSessionToken"),
            json_dict.get("AwsRegionName"),
            json_dict.get("RemoteFolderName"),
            json_dict.get("BucketName"),
            json_dict.get("CustomServerUrl"),
            json_dict.get("ForcePathStyle"),
            S3StorageClass(storage_class_raw) if storage_class_raw is not None else None,
            json_dict.get("DisableChecksumValidation", False),
        )

    def to_json(self) -> dict:
        result = {
            "AwsAccessKey": self.aws_access_key,
            "AwsSecretKey": self.aws_secret_key,
            "AwsSessionToken": self.aws_session_token,
            "AwsRegionName": self.aws_region_name,
            "RemoteFolderName": self.remote_folder_name,
            "BucketName": self.bucket_name,
            "CustomServerUrl": self.custom_server_url,
            "ForcePathStyle": self.force_path_style,
            "DisableChecksumValidation": self.disable_checksum_validation,
        }
        if self.storage_class is not None:
            result["StorageClass"] = self.storage_class.value
        return result

    def __eq__(self, other) -> bool:
        if not isinstance(other, RemoteAttachmentsS3Settings):
            return False

        return (
            self.aws_region_name == other.aws_region_name
            and self.bucket_name == other.bucket_name
            and self.remote_folder_name == other.remote_folder_name
            and self.custom_server_url == other.custom_server_url
            and self.force_path_style == other.force_path_style
            and self.disable_checksum_validation == other.disable_checksum_validation
            and self.storage_class == other.storage_class
            and self.aws_access_key == other.aws_access_key
            and self.aws_secret_key == other.aws_secret_key
            and self.aws_session_token == other.aws_session_token
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.aws_region_name,
                self.bucket_name,
                self.remote_folder_name,
                self.custom_server_url,
                self.force_path_style,
                self.disable_checksum_validation,
                self.storage_class,
                self.aws_access_key,
                self.aws_secret_key,
                self.aws_session_token,
            )
        )


class RemoteAttachmentsAzureSettings:
    def __init__(
        self,
        storage_container: str = None,
        remote_folder_name: str = None,
        account_name: str = None,
        account_key: str = None,
        sas_token: str = None,
    ):
        self.storage_container = storage_container
        self.remote_folder_name = remote_folder_name
        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token

    @classmethod
    def from_json(cls, json_dict: dict) -> RemoteAttachmentsAzureSettings:
        return cls(
            json_dict.get("StorageContainer"),
            json_dict.get("RemoteFolderName"),
            json_dict.get("AccountName"),
            json_dict.get("AccountKey"),
            json_dict.get("SasToken"),
        )

    def to_json(self) -> dict:
        return {
            "StorageContainer": self.storage_container,
            "RemoteFolderName": self.remote_folder_name,
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "SasToken": self.sas_token,
        }


class RemoteAttachmentsDestinationConfiguration:
    def __init__(
        self,
        disabled: bool = False,
        s3_settings: Optional[RemoteAttachmentsS3Settings] = None,
        azure_settings: Optional[RemoteAttachmentsAzureSettings] = None,
    ):
        self.disabled = disabled
        self.s3_settings = s3_settings
        self.azure_settings = azure_settings

    @classmethod
    def from_json(cls, json_dict: dict) -> RemoteAttachmentsDestinationConfiguration:
        s3_raw = json_dict.get("S3Settings")
        azure_raw = json_dict.get("AzureSettings")
        return cls(
            json_dict.get("Disabled", False),
            RemoteAttachmentsS3Settings.from_json(s3_raw) if s3_raw is not None else None,
            RemoteAttachmentsAzureSettings.from_json(azure_raw) if azure_raw is not None else None,
        )

    def _is_s3_configured(self) -> bool:
        return self.s3_settings is not None and bool(self.s3_settings.bucket_name)

    def _is_azure_configured(self) -> bool:
        return (
            self.azure_settings is not None
            and bool(self.azure_settings.account_name)
            and bool(self.azure_settings.storage_container)
        )

    def assert_configuration(self, key: str, database_name: str = None) -> None:
        db_str = f" for database '{database_name}'" if database_name else ""
        if not self._is_s3_configured() and not self._is_azure_configured():
            raise ValueError(f"Exactly one uploader for RemoteAttachmentsConfiguration{db_str} must be configured.")
        if self._is_s3_configured() and self._is_azure_configured():
            raise ValueError(f"Only one uploader for RemoteAttachmentsConfiguration{db_str} can be configured.")

    def to_json(self) -> dict:
        return {
            "Disabled": self.disabled,
            "S3Settings": self.s3_settings.to_json() if self.s3_settings is not None else None,
            "AzureSettings": self.azure_settings.to_json() if self.azure_settings is not None else None,
        }


class RemoteAttachmentsConfiguration:
    def __init__(
        self,
        destinations: Optional[Dict[str, RemoteAttachmentsDestinationConfiguration]] = None,
        check_frequency_in_sec: Optional[int] = None,
        max_items_to_process: Optional[int] = None,
        concurrent_uploads: Optional[int] = None,
        disabled: bool = False,
    ):
        self.destinations = destinations if destinations is not None else {}
        self.check_frequency_in_sec = check_frequency_in_sec
        self.max_items_to_process = max_items_to_process
        self.concurrent_uploads = concurrent_uploads
        self.disabled = disabled

    @classmethod
    def from_json(cls, json_dict: dict) -> RemoteAttachmentsConfiguration:
        destinations_raw = json_dict.get("Destinations") or {}
        destinations = {k: RemoteAttachmentsDestinationConfiguration.from_json(v) for k, v in destinations_raw.items()}
        return cls(
            destinations,
            json_dict.get("CheckFrequencyInSec"),
            json_dict.get("MaxItemsToProcess"),
            json_dict.get("ConcurrentUploads"),
            json_dict.get("Disabled", False),
        )

    def assert_configuration(self, database_name: str = None) -> None:
        db_str = f" for database '{database_name}'" if database_name else ""

        if self.check_frequency_in_sec is not None and self.check_frequency_in_sec <= 0:
            raise ValueError(f"Remote attachments check frequency{db_str} must be greater than 0.")
        if self.max_items_to_process is not None and self.max_items_to_process <= 0:
            raise ValueError(f"Max items to process{db_str} must be greater than 0.")
        if self.concurrent_uploads is not None and self.concurrent_uploads <= 0:
            raise ValueError(f"Concurrent attachments uploads{db_str} must be greater than 0.")

        if not self.destinations:
            return

        seen_keys = set()
        for key, dest in self.destinations.items():
            lower_key = key.lower()
            if lower_key in seen_keys:
                raise ValueError(
                    f"Destination key '{key}' is duplicate. Duplicate keys are not allowed in remote attachments configuration{db_str}."
                )
            seen_keys.add(lower_key)
            if dest is None:
                raise ValueError(f"Destination configuration for key {key} is null{db_str}.")
            dest.assert_configuration(key, database_name)

    def to_json(self) -> dict:
        return {
            "Destinations": {k: v.to_json() for k, v in self.destinations.items()} if self.destinations else {},
            "CheckFrequencyInSec": self.check_frequency_in_sec,
            "MaxItemsToProcess": self.max_items_to_process,
            "ConcurrentUploads": self.concurrent_uploads,
            "Disabled": self.disabled,
        }


class RemoteAttachmentParameters:
    def __init__(self, identifier: str, at: datetime):
        if not identifier or identifier.isspace():
            raise ValueError("Attachment identifier cannot be None or whitespace.")
        if at is None or at == datetime.min:
            raise ValueError("Attachment upload date cannot be default value.")
        self.identifier = identifier
        self.at = at
        self.flags = RemoteAttachmentFlags.NONE

    @classmethod
    def from_json(cls, json_dict: dict) -> RemoteAttachmentParameters:
        obj = cls.__new__(cls)
        obj.identifier = json_dict["Identifier"]
        obj.at = Utils.string_to_datetime(json_dict["At"])
        obj.flags = RemoteAttachmentFlags.from_str(json_dict.get("Flags", "None"))
        return obj

    def to_json(self) -> dict:
        return {
            "At": Utils.datetime_to_string(self.at),
            "Identifier": self.identifier,
            "Flags": self.flags.to_str(),
        }


class ConfigureRemoteAttachmentsOperationResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: dict) -> ConfigureRemoteAttachmentsOperationResult:
        return cls(json_dict.get("RaftCommandIndex"))


class ConfigureRemoteAttachmentsOperation(MaintenanceOperation[ConfigureRemoteAttachmentsOperationResult]):
    def __init__(self, configuration: RemoteAttachmentsConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None.")
        configuration.assert_configuration()
        self.__configuration = configuration

    def get_command(
        self, conventions: "DocumentConventions"
    ) -> RavenCommand[ConfigureRemoteAttachmentsOperationResult]:
        return self.__ConfigureAttachmentsRemoteCommand(self.__configuration)

    class __ConfigureAttachmentsRemoteCommand(RavenCommand[ConfigureRemoteAttachmentsOperationResult], RaftCommand):
        def __init__(self, configuration: RemoteAttachmentsConfiguration):
            super().__init__(ConfigureRemoteAttachmentsOperationResult)
            if configuration is None:
                raise ValueError("Configuration cannot be None.")
            self.__configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request(
                "PUT",
                f"{node.url}/databases/{node.database}/admin/attachments/remote/config",
                data=self.__configuration.to_json(),
            )
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ConfigureRemoteAttachmentsOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetRemoteAttachmentsConfigurationOperation(MaintenanceOperation[RemoteAttachmentsConfiguration]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[RemoteAttachmentsConfiguration]:
        return self.__GetRemoteAttachmentsConfigurationCommand()

    class __GetRemoteAttachmentsConfigurationCommand(RavenCommand[RemoteAttachmentsConfiguration]):
        def __init__(self):
            super().__init__(RemoteAttachmentsConfiguration)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "GET",
                f"{node.url}/databases/{node.database}/admin/attachments/remote/config",
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return
            self.result = RemoteAttachmentsConfiguration.from_json(json.loads(response))
