from copy import deepcopy
from typing import Union, List, Dict, TYPE_CHECKING, Optional

from ravendb import constants
from ravendb.documents.commands.batches import SingleNodeBatchCommand, ClusterWideBatchCommand, CommandType
from ravendb.documents.operations.patch import PatchStatus
from ravendb.documents.session.event_args import AfterSaveChangesEventArgs
from ravendb.documents.session.misc import TransactionMode
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.exceptions.raven_exceptions import ClientVersionMismatchException
from ravendb.json.result import BatchCommandResult
from ravendb.tools.utils import CaseInsensitiveDict

if TYPE_CHECKING:
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class BatchOperation:
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.__session = session
        self.__entities: List[object] = []
        self.__session_commands_count: Union[None, int] = None
        self.__all_commands_count: Union[None, int] = None
        self.__on_successful_request: Union[
            None, "InMemoryDocumentSessionOperations.SaveChangesData.ActionsToRunOnSuccess"
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
            return ClusterWideBatchCommand(
                self.__session.conventions,
                result.session_commands,
                result.options,
                self.__session.disable_atomic_document_writes_in_cluster_wide_transaction,
            )
        return SingleNodeBatchCommand(self.__session.conventions, result.session_commands, result.options)

    def set_result(self, result: BatchCommandResult) -> None:
        def get_command_type(obj_node: dict) -> CommandType:
            c_type = obj_node.get("Type")
            if not c_type:
                return CommandType.NONE

            type_as_str = str(c_type)

            command_type = CommandType.from_csharp_value_str(type_as_str)
            return command_type

        if result.results is None:
            self.__throw_on_null_result()
            return

        self.__on_successful_request.clear_session_state_after_successful_save_changes()

        if self.__session.transaction_mode == TransactionMode.CLUSTER_WIDE:
            if result.transaction_index <= 0:
                raise ClientVersionMismatchException(
                    "Cluster transaction was send to a node that is not supporting "
                    "it. So it was executed ONLY on the requested node on " + self.__session._request_executor.url
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

        cluster_session = self.__session.cluster_transaction
        cluster_session.update_state(key, index)

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
            session_document_info = self.__session._documents_by_id.get(key)
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
                self.__session.after_save_changes_invoke(
                    AfterSaveChangesEventArgs(self.__session, document_info.key, document_info.entity)
                )

    def __handle_delete(self, batch_result: dict) -> None:
        self.__handle_delete_internal(batch_result, CommandType.DELETE)

    def __handle_delete_internal(self, batch_result: dict, command_type: CommandType):
        key = self.__get_string_field(batch_result, command_type, "Id")
        document_info = self.__session._documents_by_id.get(key)
        if document_info is None:
            return

        self.__session._documents_by_id.pop(key, None)

        if document_info.entity is not None:
            self.__session._documents_by_entity.pop(document_info.entity, None)
            self.__session._deleted_entities.discard(document_info.entity)

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

        document_info = self.__session._documents_by_id.get(key)
        if not document_info:
            return

        document_info.change_vector = change_vector

        self.__handle_metadata_modifications(document_info, batch_result, key, change_vector)
        self.__session.__on_after_save_changes(self.__session, document_info.key, document_info.entity)

    def __handle_put(self, index: int, batch_result: dict, is_deferred: bool) -> None:
        entity = None
        document_info = None

        if not is_deferred:
            entity = self.__entities[index]
            document_info = self.__session._documents_by_entity.get(entity)
            if document_info is None:
                return

        key = self.__get_string_field(batch_result, CommandType.PUT, constants.Documents.Metadata.ID)
        change_vector = self.__get_string_field(
            batch_result, CommandType.PUT, constants.Documents.Metadata.CHANGE_VECTOR
        )

        if is_deferred:
            session_document_info = self.__session._documents_by_id.get(key)
            if session_document_info is None:
                return

            document_info = self.__get_or_add_modifications(key, session_document_info, True)
            entity = document_info.entity

        self.__handle_metadata_modifications(document_info, batch_result, key, change_vector)

        self.__session._documents_by_id.update({document_info.key: document_info})

        if entity:
            self.__session.generate_entity_id_on_the_client.try_set_identity(entity, key)

        self.__session.after_save_changes_invoke(
            AfterSaveChangesEventArgs(self.__session, document_info.key, document_info.entity)
        )

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
            document_info = self.__session._documents_by_id.get(doc_id)
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

        session_document_info = self.__session._documents_by_id.get_value(key)
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

        session_document_info = self.__session._documents_by_id.get_value(key)
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
