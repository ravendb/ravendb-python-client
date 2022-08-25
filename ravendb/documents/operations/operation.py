# todo: DatabaseChanges class
import time
from typing import Callable, TYPE_CHECKING, Optional

from ravendb.documents.operations.definitions import OperationExceptionResult
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.http.raven_command import RavenCommand
from ravendb.primitives.exceptions import OperationCancelledException
from ravendb.tools.utils import Utils
from ravendb.documents.operations.misc import GetOperationStateOperation

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.changes.database_changes import DatabaseChanges


class Operation:
    def __init__(
        self,
        request_executor: "RequestExecutor",
        changes: Optional[Callable[[], "DatabaseChanges"]],
        conventions: "DocumentConventions",
        key: int,
        node_tag: str = None,
    ):
        self.__request_executor = request_executor
        self.__conventions = conventions
        self.__key = key
        self.node_tag = node_tag

    def fetch_operations_status(self) -> dict:
        command = self._get_operation_state_command(self.__conventions, self.__key, self.node_tag)
        self.__request_executor.execute_command(command)

        return command.result

    def _get_operation_state_command(
        self, conventions: "DocumentConventions", key: int, node_tag: str = None
    ) -> RavenCommand[dict]:
        return GetOperationStateOperation.GetOperationStateCommand(self.__key, node_tag)

    def wait_for_completion(self) -> None:
        while True:
            status = self.fetch_operations_status()
            operation_status = status.get("Status")

            if operation_status == "Completed":
                return
            elif operation_status == "Canceled":
                raise OperationCancelledException()
            elif operation_status == "Faulted":
                result = status.get("Result")
                exception_result: OperationExceptionResult = Utils.initialize_object(
                    result, OperationExceptionResult, True
                )
                schema = ExceptionDispatcher.ExceptionSchema(
                    self.__request_executor.url, exception_result.type, exception_result.message, exception_result.error
                )
                raise ExceptionDispatcher.get(schema, exception_result.status_code)

            time.sleep(0.5)
