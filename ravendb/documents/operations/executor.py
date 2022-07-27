from __future__ import annotations

from typing import Union, Optional, TYPE_CHECKING, TypeVar

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
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.documents import DocumentStore


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


class SessionOperationExecutor(OperationExecutor):
    def __init__(self, session: InMemoryDocumentSessionOperations):
        super().__init__(session._document_store, session.database_name)
        self.__session = session

    def for_database(self, database_name: str) -> OperationExecutor:
        raise RuntimeError("The method is not supported")


class MaintenanceOperationExecutor:
    def __init__(self, store: DocumentStore, database_name: Optional[str] = None):
        self.__store = store
        self.__database_name = database_name or store.database
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
        if database_name is not None and self.__database_name.lower() == database_name.lower():
            return self

        return MaintenanceOperationExecutor(self.__store, database_name)

    def send(
        self, operation: Union[VoidMaintenanceOperation, MaintenanceOperation[_Operation_T]]
    ) -> Optional[_Operation_T]:
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
