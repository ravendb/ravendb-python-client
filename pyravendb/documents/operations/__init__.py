from __future__ import annotations

import datetime
import json
from abc import abstractmethod
from copy import deepcopy
from enum import Enum
from typing import Union, Optional, TYPE_CHECKING, Generic, TypeVar, Dict, List

import requests

from pyravendb import constants
from pyravendb.data.query import IndexQuery
from pyravendb.documents.operations.operation import Operation
from pyravendb.documents.session import SessionInfo
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.transaction_mode import TransactionMode
from pyravendb.exceptions.raven_exceptions import ClientVersionMismatchException
from pyravendb.http import ServerNode, VoidRavenCommand
from pyravendb.http.http_cache import HttpCache
from pyravendb.http.raven_command import RavenCommand
from pyravendb.json.result import BatchCommandResult
from pyravendb.serverwide.server_operation_executor import ServerOperationExecutor
from pyravendb.tools.utils import CaseInsensitiveDict, Utils
from pyravendb.documents.commands.batches import SingleNodeBatchCommand, ClusterWideBatchCommand, CommandType

if TYPE_CHECKING:
    from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
    from pyravendb.data.document_conventions import DocumentConventions
    from pyravendb.http.request_executor import RequestExecutor
    from pyravendb.documents import DocumentStore


_T = TypeVar("_T")
_Operation_T = TypeVar("_Operation_T")


class OperationExecutor:
    def __init__(self, store: DocumentStore, database_name: str = None):
        self.__store = store
        self.__database_name = database_name if database_name else store.database
        if not self.__database_name.isspace():
            self.__request_executor = store.get_request_executor(self.__database_name)
        else:
            raise ValueError("Cannot use operations without a database defined, did you forget to call 'for_database'?")

    def for_database(self, database_name: str) -> OperationExecutor:
        if self.__database_name.lower() == database_name.lower():
            return self
        return OperationExecutor(self.__store, database_name)

    def send(self, operation: Union[IOperation, VoidOperation], session_info: SessionInfo = None):
        command = operation.get_command(
            self.__store, self.__request_executor.conventions, self.__request_executor.cache
        )
        self.__request_executor.execute_command(command, session_info)
        return None if isinstance(operation, VoidOperation) else command.result

    def send_async(self, operation: IOperation[OperationIdResult]) -> Operation:
        command = operation.get_command(
            self.__store, self.__request_executor.conventions, self.__request_executor.cache
        )
        self.__request_executor.execute_command(command)
        node = command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag
        return Operation(
            self.__request_executor,
            lambda: None,
            self.__request_executor.conventions,
            command.result.operation_id,
            node,
        )

    # todo: send patch operations - create send_patch method
    #  or
    #  refactor 'send' methods above to act different while taking different sets of args
    #  (see jvmravendb OperationExecutor.java line 83-EOF)


class MaintenanceOperationExecutor:
    def __init__(self, store: DocumentStore, database_name: Optional[str] = None):
        self.__store = store
        self.__database_name = database_name if database_name is not None else store.database
        self.__request_executor: Union[None, RequestExecutor] = None
        self.__server_operation_executor: Union[ServerOperationExecutor, None] = None

    def __get_request_executor(self) -> RequestExecutor:
        if self.__request_executor is not None:
            return self.__request_executor

        self.__request_executor = (
            self.__store.get_request_executor(self.__database_name) if self.__database_name else None
        )
        return self.__request_executor

    @property
    def server(self) -> ServerOperationExecutor:
        if self.__server_operation_executor is not None:
            return self.__server_operation_executor
        self.__server_operation_executor = ServerOperationExecutor(self.__store)
        return self.__server_operation_executor

    def for_database(self, database_name: str) -> MaintenanceOperationExecutor:
        if self.__database_name.lower() == database_name.lower():
            return self

        return MaintenanceOperationExecutor(self.__store, database_name)

    def send(self, operation: Union[VoidMaintenanceOperation, MaintenanceOperation]):
        self.__assert_database_name_set()
        command = operation.get_command(self.__get_request_executor().conventions)
        self.__get_request_executor().execute_command(command)
        return None if isinstance(operation, VoidMaintenanceOperation) else command.result

    def send_async(self, operation: MaintenanceOperation[OperationIdResult]) -> Operation:
        self.__assert_database_name_set()
        command = operation.get_command(self.__get_request_executor().conventions)

        self.__get_request_executor().execute_command(command)
        node = command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag
        return Operation(
            self.__get_request_executor(),
            #  todo : changes
            #  lambda: self.__store.changes(self.__database_name, node),
            lambda: None,
            self.__get_request_executor().conventions,
            command.result.operation_id,
            node,
        )

    def __assert_database_name_set(self) -> None:
        if self.__database_name is None:
            raise ValueError(
                "Cannot use maintenance without a database defined, did you forget to call 'for_database'?"
            )


# todo: research if it is necessary to create such class that
#  mimics Operation interface and isn't an Operation class at the same time
class IOperation(Generic[_Operation_T]):
    def get_command(
        self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache
    ) -> RavenCommand[_Operation_T]:
        pass


class VoidOperation(IOperation[None]):
    @abstractmethod
    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> VoidRavenCommand:
        pass


class MaintenanceOperation(Generic[_T]):
    @abstractmethod
    def get_command(self, conventions: DocumentConventions) -> RavenCommand[_T]:
        pass


class VoidMaintenanceOperation(MaintenanceOperation[None]):
    @abstractmethod
    def get_command(self, conventions: DocumentConventions) -> VoidRavenCommand:
        pass


class OperationIdResult:
    def __init__(self, operation_id: int = None, operation_node_tag: str = None):
        self.operation_id = operation_id
        self.operation_node_tag = operation_node_tag


class OperationExceptionResult:
    def __init__(self, type: str, message: str, error: str, status_code: int):
        self.type = type
        self.message = message
        self.error = error
        self.status_code = status_code


class GetOperationStateOperation(MaintenanceOperation):
    def __init__(self, key: int, node_tag: str = None):
        self.__key = key
        self.__node_tag = node_tag

    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self.GetOperationStateCommand(self.__key, self.__node_tag)

    class GetOperationStateCommand(RavenCommand[dict]):
        def __init__(self, key: int, node_tag: str = None):
            super().__init__(dict)
            self.__key = key
            self.__selected_node_tag = node_tag

            self.__timeout = datetime.timedelta(seconds=15)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{node.url}/databases/{node.database}/operations/state?id={self.__key}")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = json.loads(response)


class BatchOperation:
    def __init__(self, session: InMemoryDocumentSessionOperations):
        self.__session = session
        self.__entities: List[object] = []
        self.__session_commands_count: Union[None, int] = None
        self.__all_commands_count: Union[None, int] = None
        self.__on_successful_request: Union[
            None, InMemoryDocumentSessionOperations.SaveChangesData.ActionsToRunOnSuccess
        ] = None
        self.__modifications: Union[None, Dict[str, DocumentInfo]] = None

    def create_request(self) -> Union[None, SingleNodeBatchCommand]:
        result = self.__session.prepare_for_save_changes()
        self.__on_successful_request = result.on_success
        self.__session_commands_count = len(result.session_commands)
        result.session_commands.extend(result.deferred_commands)

        self.__session.validate_cluster_transaction(result)

        self.__all_commands_count = len(result.session_commands)

        if self.__all_commands_count == 0:
            return None

        self.__session.increment_requests_count()

        self.__entities = result.entities

        if self.__session.transaction_mode == TransactionMode.CLUSTER_WIDE:
            return ClusterWideBatchCommand(self.__session.conventions, result.session_commands, result.options)
        return SingleNodeBatchCommand(self.__session.conventions, result.session_commands, result.options)

    def set_result(self, result: BatchCommandResult) -> None:
        def get_command_type(obj_node: dict) -> CommandType:
            c_type = obj_node.get("Type")
            if not c_type:
                return CommandType.NONE

            type_as_str = str(c_type)

            command_type = CommandType.parse_csharp_value(type_as_str)
            return command_type

        if result.results is None:
            self.__throw_on_null_result()
            return

        self.__on_successful_request.clear_session_state_after_successful_save_changes()

        if self.__session.transaction_mode == TransactionMode.CLUSTER_WIDE:
            if result.transaction_index <= 0:
                raise ClientVersionMismatchException(
                    "Cluster transaction was send to a node that is not supporting "
                    "it. So it was executed ONLY on the requested node on " + self.__session.request_executor.url
                )

        for i in range(self.__session_commands_count):
            batch_result = result.results[i]
            if batch_result is None:
                continue

            command_type = get_command_type(batch_result)

            if command_type == CommandType.PUT:
                self.__handle_put(i, batch_result, False)
            elif command_type == CommandType.FORCE_REVISION_CREATION:
                self.__handle_force_revision_creation(batch_result)
            elif command_type == CommandType.DELETE:
                self.__handle_delete(batch_result)
            elif command_type == CommandType.COMPARE_EXCHANGE_PUT:
                self.__handle_compare_exchange_put(batch_result)
            elif command_type == CommandType.COMPARE_EXCHANGE_DELETE:
                self.__handle_compare_exchange_delete(batch_result)
            else:
                raise ValueError(f"Command {command_type} is not supported")

        for i in range(self.__session_commands_count, self.__all_commands_count):
            batch_result = result.results[i]
            if batch_result is None:
                continue

            command_type = get_command_type(batch_result)
            if command_type == CommandType.PUT:
                self.__handle_put(i, batch_result, False)
            elif command_type == CommandType.DELETE:
                self.__handle_delete(batch_result)
            elif command_type == CommandType.PATCH:
                self.__handle_patch(batch_result)
            elif command_type == CommandType.ATTACHMENT_PUT:
                self.__handle_attachment_put(batch_result)
            elif command_type == CommandType.ATTACHMENT_DELETE:
                self.__handle_attachment_delete(batch_result)
            elif command_type == CommandType.ATTACHMENT_MOVE:
                self.__handle_attachment_move(batch_result)
            elif command_type == CommandType.ATTACHMENT_COPY:
                self.__handle_attachment_copy(batch_result)
            elif (
                command_type == CommandType.COMPARE_EXCHANGE_PUT
                or CommandType.COMPARE_EXCHANGE_DELETE
                or CommandType.FORCE_REVISION_CREATION
            ):
                pass
            elif command_type == CommandType.COUNTERS:
                self.__handle_counters(batch_result)
            elif command_type == CommandType.TIME_SERIES_COPY or command_type == CommandType.BATCH_PATCH:
                break
            else:
                raise ValueError(f"Command {command_type} is not supported")

        self.__finalize_result()

    def __finalize_result(self):
        if not self.__modifications:
            return

        for key, document_info in self.__modifications.items():
            self.__apply_metadata_modifications(key, document_info)

    def __apply_metadata_modifications(self, key: str, document_info: DocumentInfo):
        document_info.metadata_instance = None
        document_info.metadata = deepcopy(document_info.metadata)
        document_info.metadata[constants.Documents.Metadata.CHANGE_VECTOR] = document_info.change_vector
        document_copy = deepcopy(document_info.document)
        document_copy[constants.Documents.Metadata.KEY] = document_info.metadata

        document_info.document = document_copy

    def __get_or_add_modifications(
        self, key: str, document_info: DocumentInfo, apply_modifications: bool
    ) -> DocumentInfo:
        if not self.__modifications:
            self.__modifications = CaseInsensitiveDict()

        modified_document_info = self.__modifications.get(key)
        if modified_document_info is not None:
            if apply_modifications:
                self.__apply_metadata_modifications(key, modified_document_info)
        else:
            self.__modifications[key] = modified_document_info = document_info

        return modified_document_info

    def __handle_compare_exchange_put(self, batch_result: dict) -> None:
        self.__handle_compare_exchange_internal(CommandType.COMPARE_EXCHANGE_PUT, batch_result)

    def __handle_compare_exchange_delete(self, batch_result: dict) -> None:
        self.__handle_compare_exchange_internal(CommandType.COMPARE_EXCHANGE_DELETE, batch_result)

    def __handle_compare_exchange_internal(self, command_type: CommandType, batch_result: dict) -> None:
        key: str = batch_result.get("Key")
        if not key:
            self.__throw_missing_field(command_type, "Key")

        index: int = batch_result.get("Index")
        if not index:
            self.__throw_missing_field(command_type, "Index")

        cluster_session = self.__session.get_cluster_session()
        cluster_session.update_state(key, index)

    # todo: handle attachment stuff

    def __handle_patch(self, batch_result: dict) -> None:
        patch_status = batch_result.get("PatchStatus")
        if not patch_status:
            self.__throw_missing_field(CommandType.PATCH, "PatchStatus")

        status = PatchStatus(patch_status)
        if status == PatchStatus.CREATED or PatchStatus.PATCHED:
            document = batch_result.get("ModifiedDocument")
            if not document:
                return

            key = self.__get_string_field(batch_result, CommandType.PUT, "Id")
            session_document_info = self.__session.documents_by_id.get(key)
            if session_document_info == None:
                return

            document_info = self.__get_or_add_modifications(key, session_document_info, True)

            change_vector = self.__get_string_field(batch_result, CommandType.PATCH, "ChangeVector")
            last_modified = self.__get_string_field(batch_result, CommandType.PATCH, "LastModified")

            document_info.change_vector = change_vector
            document_info.metadata[constants.Documents.Metadata.KEY] = key
            document_info.metadata[constants.Documents.Metadata.CHANGE_VECTOR] = change_vector
            document_info.metadata[constants.Documents.Metadata.LAST_MODIFIED] = last_modified

            document_info.document = document
            self.__apply_metadata_modifications(key, document_info)

            if document_info.entity is not None:
                self.__session.entity_to_json.populate_entity(document_info.entity, key, document_info.document)
                self.__session.on_after_save_changes(self.__session, document_info.key, document_info.entity)

    def __handle_delete(self, batch_result: dict) -> None:
        self.__handle_delete_internal(batch_result, CommandType.DELETE)

    def __handle_delete_internal(self, batch_result: dict, command_type: CommandType):
        key = self.__get_string_field(batch_result, command_type, "Id")
        document_info = self.__session.documents_by_id.get(key)
        if document_info is None:
            return

        self.__session.documents_by_id.pop(key, None)

        if document_info.entity is not None:
            self.__session.documents_by_entity.pop(document_info.entity, None)
            self.__session.deleted_entities.discard(document_info.entity)

    def __handle_force_revision_creation(self, batch_result: dict) -> None:
        if not self.__get_boolean_field(batch_result, CommandType.FORCE_REVISION_CREATION, "RevisionCreated"):
            # no forced revision was created...nothing to update
            return

        key = self.__get_string_field(
            batch_result, CommandType.FORCE_REVISION_CREATION, constants.Documents.Metadata.KEY
        )
        change_vector = self.__get_string_field(
            batch_result, CommandType.FORCE_REVISION_CREATION, constants.Documents.Metadata.CHANGE_VECTOR
        )

        document_info = self.__session.documents_by_id.get(key)
        if not document_info:
            return

        document_info.change_vector = change_vector

        self.__handle_metadata_modifications(document_info, batch_result, key, change_vector)
        self.__session.on_after_save_changes(self.__session, document_info.key, document_info.entity)

    def __handle_put(self, index: int, batch_result: dict, is_deferred: bool) -> None:
        entity = None
        document_info = None

        if not is_deferred:
            entity = self.__entities[index]
            document_info = self.__session.documents_by_entity.get(entity)
            if document_info is None:
                return

        key = self.__get_string_field(batch_result, CommandType.PUT, constants.Documents.Metadata.ID)
        change_vector = self.__get_string_field(
            batch_result, CommandType.PUT, constants.Documents.Metadata.CHANGE_VECTOR
        )

        if is_deferred:
            session_document_info = self.__session.documents_by_id.get(key)
            if session_document_info is None:
                return

            document_info = self.__get_or_add_modifications(key, session_document_info, True)
            entity = document_info.entity

        self.__handle_metadata_modifications(document_info, batch_result, key, change_vector)

        self.__session.documents_by_id.update({document_info.key: document_info})

        if entity:
            self.__session.generate_entity_id_on_the_client.try_set_identity(entity, key)

        self.__session.on_after_save_changes(self.__session, document_info.key, document_info.entity)

    def __handle_metadata_modifications(
        self, document_info: DocumentInfo, batch_result: dict, key: str, change_vector: str
    ) -> None:
        for property_name, value in batch_result.items():
            if "Type" == property_name:
                continue

            document_info.metadata[property_name] = value

        document_info.key = key
        document_info.change_vector = change_vector

        self.__apply_metadata_modifications(key, document_info)

    def __handle_counters(self, batch_result: dict) -> None:
        doc_id = self.__get_string_field(batch_result, CommandType.COUNTERS, "Id")
        counters_detail: dict = batch_result.get("CountersDetail")
        if counters_detail is None:
            self.__throw_missing_field(CommandType.COUNTERS, "CountersDetail")

        counters = counters_detail.get("Counters")
        if counters is None:
            self.__throw_missing_field(CommandType.COUNTERS, "Counters")

        cache = self.__session.counters_by_doc_id[doc_id]
        if cache is None:
            cache = [False, CaseInsensitiveDict()]
            self.__session.counters_by_doc_id[doc_id] = cache

        change_vector = self.__get_string_field(batch_result, CommandType.COUNTERS, "DocumentChangeVector", False)
        if change_vector is not None:
            document_info = self.__session.documents_by_id.get(doc_id)
            if document_info is not None:
                document_info.change_vector = change_vector

        for counter in counters:
            counter: dict
            name = counter.get("CounterName")
            value = counter.get("TotalValue")

            if not name and not value:
                cache[1][name] = value

    def __handle_attachment_put(self, batch_result: dict) -> None:
        self.__handle_attachment_put_internal(
            batch_result, CommandType.ATTACHMENT_PUT, "Id", "Name", "DocumentChangeVector"
        )

    def __handle_attachment_copy(self, batch_result: dict) -> None:
        self.__handle_attachment_put_internal(
            batch_result, CommandType.ATTACHMENT_COPY, "Id", "Name", "DocumentChangeVector"
        )

    def __handle_attachment_move(self, batch_result: dict) -> None:
        self.__handle_attachment_delete_internal(
            batch_result, CommandType.ATTACHMENT_MOVE, "Id", "Name", "DocumentChangeVector"
        )
        self.__handle_attachment_put_internal(
            batch_result, CommandType.ATTACHMENT_MOVE, "DestinationId", "DestinationName", "DocumentChangeVector"
        )

    def __handle_attachment_delete(self, batch_result: dict) -> None:
        self.__handle_attachment_delete_internal(
            batch_result, CommandType.ATTACHMENT_DELETE, constants.Documents.Metadata.ID, "Name", "DocumentChangeVector"
        )

    def __handle_attachment_delete_internal(
        self,
        batch_result: dict,
        command_type: CommandType,
        id_field_name: str,
        attachment_name_field_name: str,
        document_change_vector_field_name: str,
    ) -> None:
        key = self.__get_string_field(batch_result, command_type, id_field_name)

        session_document_info = self.__session.documents_by_id.get_value(key)
        if session_document_info is None:
            return

        document_info = self.__get_or_add_modifications(key, session_document_info, True)

        document_change_vector = self.__get_string_field(
            batch_result, command_type, document_change_vector_field_name, False
        )
        if document_change_vector:
            document_info.change_vector = document_change_vector

        attachments_json = document_info.metadata.get(constants.Documents.Metadata.ATTACHMENTS)
        if not attachments_json:
            return

        name = self.__get_string_field(batch_result, command_type, attachment_name_field_name)

        attachments = []
        document_info.metadata[constants.Documents.Metadata.ATTACHMENTS] = attachments

        for attachment in attachments_json:
            attachment_name = self.__get_string_field(attachment, command_type, "Name")
            if attachment_name == name:
                continue

            attachments.append(attachment)

    def __handle_attachment_put_internal(
        self,
        batch_result: dict,
        command_type: CommandType,
        id_field_name: str,
        attachment_name_field_name: str,
        document_change_vector_field_name: str,
    ) -> None:
        key = self.__get_string_field(batch_result, command_type, id_field_name)

        session_document_info = self.__session.documents_by_id.get_value(key)
        if session_document_info is None:
            return

        document_info = self.__get_or_add_modifications(key, session_document_info, False)
        document_change_vector = self.__get_string_field(
            batch_result, command_type, document_change_vector_field_name, False
        )
        if document_change_vector:
            document_info.change_vector = document_change_vector

        attachments = document_info.metadata.get(constants.Documents.Metadata.ATTACHMENTS)
        if attachments is None:
            attachments = []
            document_info.metadata[constants.Documents.Metadata.ATTACHMENTS] = attachments

        dynamic_node = {
            "ChangeVector": self.__get_string_field(batch_result, command_type, "ChangeVector"),
            "ContentType": self.__get_string_field(batch_result, command_type, "ContentType"),
            "Hash": self.__get_string_field(batch_result, command_type, "Hash"),
            "Name": self.__get_string_field(batch_result, command_type, "Name"),
            "Size": self.__get_string_field(batch_result, command_type, "Size"),
        }
        attachments.append(dynamic_node)

    def __get_string_field(
        self, json: dict, command_type: CommandType, field_name: str, throw_on_missing: Optional[bool] = True
    ) -> str:
        json_node = json.get(field_name, None)
        if throw_on_missing and json_node is None:
            self.__throw_missing_field(command_type, field_name)
        return str(json_node)

    def __get_int_field(self, json: dict, command_type: CommandType, field_name: str) -> int:
        json_node = json.get(field_name)
        if (not json_node) or not isinstance(json_node, int):
            self.__throw_missing_field(command_type, field_name)
        return json_node

    def __get_boolean_field(self, json: dict, command_type: CommandType, field_name: str) -> bool:
        json_node = json.get(field_name)
        if (not json_node) or not isinstance(json_node, bool):
            self.__throw_missing_field(command_type, field_name)
        return json_node

    def __throw_on_null_result(self) -> None:
        raise ValueError(
            "Reveived empty response from the server. This is not supposed to happend and is likely a bug."
        )

    def __throw_missing_field(self, c_type: CommandType, field_name: str) -> None:
        raise ValueError(f"{c_type} response is invalid. Field '{field_name}' is missing.")


class PatchRequest:
    def __init__(self, script: Optional[str] = "", values: Optional[Dict[str, object]] = None):
        self.values: Dict[str, object] = values or {}
        self.script = script

    @staticmethod
    def for_script(script: str) -> PatchRequest:
        request = PatchRequest()
        request.script = script
        return request

    def serialize(self):
        return {"Script": self.script, "Values": self.values}


class PatchStatus(Enum):
    DOCUMENT_DOES_NOT_EXIST = "DocumentDoesNotExist"
    CREATED = "Created"
    PATCHED = "Patched"
    SKIPPED = "Skipped"
    NOT_MODIFIED = "NotModified"

    def __str__(self):
        return self.value


class PatchResult:
    def __init__(
        self,
        status: Optional[PatchStatus] = None,
        modified_document: Optional[dict] = None,
        original_document: Optional[dict] = None,
        debug: Optional[dict] = None,
        last_modified: Optional[datetime.datetime] = None,
        change_vector: Optional[str] = None,
        collection: Optional[str] = None,
    ):
        self.status = status
        self.modified_document = modified_document
        self.original_document = original_document
        self.debug = debug
        self.last_modified = last_modified
        self.change_vector = change_vector
        self.collection = collection

    @staticmethod
    def from_json(json_dict: dict) -> PatchResult:
        return PatchResult(
            PatchStatus(json_dict["Status"]),
            json_dict.get("ModifiedDocument", None),
            json_dict.get("OriginalDocument", None),
            json_dict.get("Debug", None),
            Utils.string_to_datetime(json_dict["LastModified"]),
            json_dict["ChangeVector"],
            json_dict["Collection"],
        )


class PatchOperation(IOperation[PatchResult]):
    class Payload:
        def __init__(self, patch: PatchRequest, patch_if_missing: PatchResult):
            self.__patch = patch
            self.__patch_if_missing = patch_if_missing

        @property
        def patch(self):
            return self.__patch

        @property
        def patch_if_missing(self):
            return self.__patch_if_missing

    class Result(Generic[_Operation_T]):
        def __init__(self, status: PatchStatus, document: _Operation_T):
            self.status = status
            self.document = document

    def __init__(
        self,
        key: str,
        change_vector: str,
        patch: PatchRequest,
        patch_if_missing: Optional[PatchRequest] = None,
        skip_patch_if_change_vector_mismatch: Optional[bool] = None,
    ):
        if not patch:
            raise ValueError("Patch cannot be None")

        if patch.script.isspace():
            raise ValueError("Patch script cannot be None or whitespace")

        if patch_if_missing and patch_if_missing.script.isspace():
            raise ValueError("PatchIfMissing script cannot be None or whitespace")

        self.__key = key
        self.__change_vector = change_vector
        self.__patch = patch
        self.__patch_if_missing = patch_if_missing
        self.__skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch

    def get_command(
        self,
        store: DocumentStore,
        conventions: DocumentConventions,
        cache: HttpCache,
        return_debug_information: Optional[bool] = False,
        test: Optional[bool] = False,
    ) -> RavenCommand[_Operation_T]:
        return self.PatchCommand(
            conventions,
            self.__key,
            self.__change_vector,
            self.__patch,
            self.__patch_if_missing,
            self.__skip_patch_if_change_vector_mismatch,
            return_debug_information,
            test,
        )

    class PatchCommand(RavenCommand[PatchResult]):
        def __init__(
            self,
            conventions: "DocumentConventions",
            document_id: str,
            change_vector: str,
            patch: PatchRequest,
            patch_if_missing: PatchRequest,
            skip_patch_if_change_vector_mismatch: Optional[bool] = False,
            return_debug_information: Optional[bool] = False,
            test: Optional[bool] = False,
        ):
            if conventions is None:
                raise ValueError("None conventions are invalid")
            if document_id is None:
                raise ValueError("None key is invalid")
            if patch is None:
                raise ValueError("None patch is invalid")
            if patch_if_missing and not patch_if_missing.script:
                raise ValueError("None or Empty script is invalid")
            if patch and patch.script.isspace():
                raise ValueError("None or empty patch_if_missing.script is invalid")
            if not document_id:
                raise ValueError("Document_id canoot be None")

            super().__init__(PatchResult)
            self.__conventions = conventions
            self.__document_id = document_id
            self.__change_vector = change_vector
            self.__patch = patch
            self.__patch_if_missing = patch_if_missing
            self.__skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch
            self.__return_debug_information = return_debug_information
            self.__test = test

        def create_request(self, server_node: ServerNode) -> requests.Request:
            path = f"docs?id={Utils.quote_key(self.__document_id)}"
            if self.__skip_patch_if_change_vector_mismatch:
                path += "&skipPatchIfChangeVectorMismatch=true"
            if self.__return_debug_information:
                path += "&debug=true"
            if self.__test:
                path += "&test=true"
            url = f"{server_node.url}/databases/{server_node.database}/{path}"
            request = requests.Request("PATCH", url)
            if self.__change_vector is not None:
                request.headers = {"If-Match": f'"{self.__change_vector}"'}

            request.data = {
                "Patch": self.__patch.serialize(),
                "PatchIfMissing": self.__patch_if_missing.serialize() if self.__patch_if_missing else None,
            }

            return request

        def is_read_request(self) -> bool:
            return False

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return

            self.result = PatchResult.from_json(json.loads(response))


class GetStatisticsOperation(MaintenanceOperation[dict]):
    def __init__(self, debug_tag: str = None, node_tag: str = None):
        self.debug_tag = debug_tag
        self.node_tag = node_tag

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[dict]:
        return self.__GetStatisticsCommand(self.debug_tag, self.node_tag)

    class __GetStatisticsCommand(RavenCommand[dict]):
        def __init__(self, debug_tag: str, node_tag: str):
            super().__init__(dict)
            self.debug_tag = debug_tag
            self.node_tag = node_tag

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "GET",
                f"{node.url}/databases/{node.database}"
                f"/stats{f'?{self.debug_tag}' if self.debug_tag is not None else ''}",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = json.loads(response)

        def is_read_request(self) -> bool:
            return True


class CollectionStatistics:
    def __init__(
        self,
        count_of_documents: Optional[int] = None,
        count_of_conflicts: Optional[int] = None,
        collections: Optional[Dict[str, int]] = None,
    ):
        self.count_of_documents = count_of_documents
        self.count_of_conflicts = count_of_conflicts
        self.collections = collections

    @staticmethod
    def from_json(json_dict: dict) -> CollectionStatistics:
        return CollectionStatistics(
            json_dict["CountOfDocuments"], json_dict["CountOfConflicts"], json_dict["Collections"]
        )


class GetCollectionStatisticsOperation(MaintenanceOperation[CollectionStatistics]):
    def get_command(self, conventions) -> RavenCommand[CollectionStatistics]:
        return self.__GetCollectionStatisticsCommand()

    class __GetCollectionStatisticsCommand(RavenCommand[CollectionStatistics]):
        def __init__(self):
            super().__init__(CollectionStatistics)

        def create_request(self, server_node: ServerNode) -> requests.Request:
            return requests.Request("GET", f"{server_node.url}/databases/{server_node.database}/collections/stats")

        def is_read_request(self) -> bool:
            return True

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = CollectionStatistics.from_json(json.loads(response))


class QueryOperationOptions(object):
    def __init__(
        self,
        allow_stale: bool = True,
        stale_timeout: datetime.timedelta = None,
        max_ops_per_sec: int = None,
        retrieve_details: bool = False,
    ):
        self.allow_stale = allow_stale
        self.stale_timeout = stale_timeout
        self.retrieve_details = retrieve_details
        self.max_ops_per_sec = max_ops_per_sec


class DeleteByQueryOperation(IOperation[OperationIdResult]):
    def __init__(self, query_to_delete: Union[str, IndexQuery], options: Optional[QueryOperationOptions] = None):
        if not query_to_delete:
            raise ValueError("Invalid query")
        if isinstance(query_to_delete, str):
            query_to_delete = IndexQuery(query=query_to_delete)
        self.__query_to_delete = query_to_delete
        self.__options = options if options is not None else QueryOperationOptions()

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: Optional[HttpCache] = None
    ) -> RavenCommand[OperationIdResult]:
        return self.__DeleteByIndexCommand(conventions, self.__query_to_delete, self.__options)

    class __DeleteByIndexCommand(RavenCommand[OperationIdResult]):
        def __init__(
            self,
            conventions: "DocumentConventions",
            query_to_delete: IndexQuery,
            options: Optional[QueryOperationOptions] = None,
        ):
            super().__init__(OperationIdResult)
            self.__conventions = conventions
            self.__query_to_delete = query_to_delete
            self.__options = options if options is not None else QueryOperationOptions()

        def create_request(self, server_node):
            url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = (
                f"?allowStale={self.__options.allow_stale}&maxOpsPerSec={self.__options.max_ops_per_sec or ''}"
                f"&details={self.__options.retrieve_details}"
            )
            if self.__options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self.__options.stale_timeout)

            url += path
            data = self.__query_to_delete.to_json()
            return requests.Request("DELETE", url, data=data)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            response = json.loads(response)
            self.result = OperationIdResult(response["OperationId"], response["OperationNodeTag"])

        def is_read_request(self) -> bool:
            return False


class PatchByQueryOperation(IOperation[OperationIdResult]):
    def __init__(self, query_to_update: Union[IndexQuery, str], options: Optional[QueryOperationOptions] = None):
        if query_to_update is None:
            raise ValueError("Invalid query")
        super().__init__()
        if isinstance(query_to_update, str):
            query_to_update = IndexQuery(query=query_to_update)
        self.__query_to_update = query_to_update
        if options is None:
            options = QueryOperationOptions()
        self.__options = options

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: Optional[HttpCache] = None
    ) -> RavenCommand[OperationIdResult]:
        return self.__PatchByQueryCommand(conventions, self.__query_to_update, self.__options)

    class __PatchByQueryCommand(RavenCommand[OperationIdResult]):
        def __init__(
            self, conventions: "DocumentConventions", query_to_update: IndexQuery, options: QueryOperationOptions
        ):
            super().__init__(OperationIdResult)
            self.__conventions = conventions
            self.__query_to_update = query_to_update
            self.__options = options

        def create_request(self, server_node) -> requests.Request:
            if not isinstance(self.__query_to_update, IndexQuery):
                raise ValueError("query must be IndexQuery Type")

            url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = (
                f"?allowStale={self.__options.allow_stale}&maxOpsPerSec={self.__options.max_ops_per_sec or ''}"
                f"&details={self.__options.retrieve_details}"
            )
            if self.__options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self.__options.stale_timeout)

            url += path
            data = {"Query": self.__query_to_update.to_json()}
            return requests.Request("PATCH", url, data=data)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            response = json.loads(response)
            self.result = OperationIdResult(response["OperationId"], response["OperationNodeTag"])

        def is_read_request(self) -> bool:
            return False
