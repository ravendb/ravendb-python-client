# todo: DatabaseChanges class
import time
from typing import Callable, TYPE_CHECKING

import pyravendb.documents.operations as doc_operations
from pyravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from pyravendb.http import RavenCommand
from pyravendb.primitives import OperationCancelledException
from pyravendb.tools.utils import Utils

if TYPE_CHECKING:
    from pyravendb.data.document_conventions import DocumentConventions
    from pyravendb.http.request_executor import RequestExecutor


class Operation:
    def __init__(
        self,
        request_executor: "RequestExecutor",
        changes: Callable[[], "DatabaseChanges"],
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
        return doc_operations.GetOperationStateOperation.GetOperationStateCommand(self.__key, node_tag)

    def wait_for_completion(self) -> None:
        while True:
            status = self.fetch_operations_status()
            # todo: check if it isn't a string at the begging - if there's a need to parse on string
            operation_status = status.get("Status")

            if operation_status == "Completed":
                return
            elif operation_status == "Canceled":
                raise OperationCancelledException()
            elif operation_status == "Faulted":
                result = status.get("Result")
                exception_result: doc_operations.OperationExceptionResult = Utils.initialize_object(
                    result, doc_operations.OperationExceptionResult, True
                )
                schema = ExceptionDispatcher.ExceptionSchema(
                    self.__request_executor.url, exception_result.type, exception_result.message, exception_result.error
                )
                raise ExceptionDispatcher.get(schema, exception_result.status_code)

            time.sleep(0.5)
