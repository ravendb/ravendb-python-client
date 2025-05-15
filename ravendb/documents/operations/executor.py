from __future__ import annotations

import http
from typing import Union, Optional, TYPE_CHECKING, TypeVar

from ravendb.documents.session.entity_to_json import EntityToJsonStatic
from ravendb.documents.operations.patch import PatchOperation, PatchStatus
from ravendb.documents.operations.definitions import (
    IOperation,
    OperationIdResult,
    VoidOperation,
    VoidMaintenanceOperation,
    MaintenanceOperation,
)
from ravendb.documents.operations.operation import Operation
from ravendb.documents.session.misc import SessionInfo
from ravendb.serverwide.server_operation_executor import ServerOperationExecutor

if TYPE_CHECKING:
    from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
        InMemoryDocumentSessionOperations,
    )
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.documents import DocumentStore


_T = TypeVar("_T")
_T_Operation_Return = TypeVar("_T_Operation_Return")


class OperationExecutor:
    def __init__(self, store: "DocumentStore", database_name: str = None):
        self._store = store
        self._database_name = database_name if database_name else store.database
        if self._database_name.isspace():
            raise ValueError("Cannot use operations without a database defined, did you forget to call 'for_database'?")
        self._request_executor = store.get_request_executor(self._database_name)

    def for_database(self, database_name: str) -> OperationExecutor:
        if self._database_name.lower() == database_name.lower():
            return self
        return OperationExecutor(self._store, database_name)

    def send(self, operation: IOperation[_T_Operation_Return], session_info: SessionInfo = None) -> _T_Operation_Return:
        command = operation.get_command(self._store, self._request_executor.conventions, self._request_executor.cache)
        self._request_executor.execute_command(command, session_info)
        return None if isinstance(operation, VoidOperation) else command.result

    def send_async(self, operation: IOperation[OperationIdResult]) -> Operation:
        command = operation.get_command(self._store, self._request_executor.conventions, self._request_executor.cache)
        self._request_executor.execute_command(command)
        node = command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag
        return Operation(
            self._request_executor,
            lambda: None,
            self._request_executor.conventions,
            command.result.operation_id,
            node,
        )

    def send_patch_operation(self, operation: PatchOperation, session_info: SessionInfo) -> PatchStatus:
        command = operation.get_command(self._store, self._request_executor.conventions, self._request_executor.cache)

        self._request_executor.execute_command(command, session_info)

        if command.status_code == http.HTTPStatus.NOT_MODIFIED:
            return PatchStatus.NOT_MODIFIED

        if command.status_code == http.HTTPStatus.NOT_FOUND:
            return PatchStatus.DOCUMENT_DOES_NOT_EXIST

        return command.result.status

    def send_patch_operation_with_entity_class(
        self, entity_class: _T, operation: PatchOperation, session_info: Optional[SessionInfo] = None
    ) -> PatchOperation.Result[_T]:
        command = operation.get_command(self._store, self._request_executor.conventions, self._request_executor.cache)

        self._request_executor.execute_command(command, session_info)

        result = PatchOperation.Result()

        if command.status_code == http.HTTPStatus.NOT_MODIFIED:
            result.status = PatchStatus.NOT_MODIFIED
            return result

        if command.status_code == http.HTTPStatus.NOT_FOUND:
            result.status = PatchStatus.DOCUMENT_DOES_NOT_EXIST
            return result

        try:
            result.status = command.result.status
            result.document = EntityToJsonStatic.convert_to_entity(
                command.result.modified_document, entity_class, self._request_executor.conventions
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Unable to read patch result: {e.args[0]}", e)


class SessionOperationExecutor(OperationExecutor):
    def __init__(self, session: InMemoryDocumentSessionOperations):
        super().__init__(session._document_store, session.database_name)
        self._session = session

    def for_database(self, database_name: str) -> OperationExecutor:
        raise RuntimeError("The method is not supported")


class MaintenanceOperationExecutor:
    def __init__(self, store: DocumentStore, database_name: Optional[str] = None):
        self._store = store
        self._database_name = database_name or store.database
        self._request_executor: Union[None, RequestExecutor] = None
        self._server_operation_executor: Union[ServerOperationExecutor, None] = None

    @property
    def request_executor(self) -> RequestExecutor:
        if self._request_executor is not None:
            return self._request_executor

        self._request_executor = self._store.get_request_executor(self._database_name) if self._database_name else None
        return self._request_executor

    @property
    def server(self) -> ServerOperationExecutor:
        if self._server_operation_executor is not None:
            return self._server_operation_executor
        self._server_operation_executor = ServerOperationExecutor.from_store(self._store)
        return self._server_operation_executor

    def for_database(self, database_name: str) -> MaintenanceOperationExecutor:
        if database_name is not None and self._database_name.lower() == database_name.lower():
            return self

        return MaintenanceOperationExecutor(self._store, database_name)

    def send(
        self, operation: Union[VoidMaintenanceOperation, MaintenanceOperation[_T_Operation_Return]]
    ) -> Optional[_T_Operation_Return]:
        self._assert_database_name_set()
        command = operation.get_command(self.request_executor.conventions)
        self.request_executor.execute_command(command)
        return None if isinstance(operation, VoidMaintenanceOperation) else command.result

    def send_async(self, operation: MaintenanceOperation[OperationIdResult]) -> Operation:
        self._assert_database_name_set()
        command = operation.get_command(self.request_executor.conventions)

        self.request_executor.execute_command(command)
        node = command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag
        return Operation(
            self.request_executor,
            #  todo : changes
            #  lambda: self.__store.changes(self.__database_name, node),
            lambda: None,
            self.request_executor.conventions,
            command.result.operation_id,
            node,
        )

    def _assert_database_name_set(self) -> None:
        if self._database_name is None:
            raise ValueError(
                "Cannot use maintenance without a database defined, did you forget to call 'for_database'?"
            )
