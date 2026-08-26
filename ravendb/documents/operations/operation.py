# todo: DatabaseChanges class
import time
from datetime import timedelta
from typing import Callable, TYPE_CHECKING, Optional

from ravendb.documents.operations.definitions import OperationExceptionResult
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.http.raven_command import RavenCommand
from ravendb.primitives.exceptions import OperationCancelledException
from ravendb.tools.utils import Utils
from ravendb.documents.operations.misc import GetOperationStateOperation


class BulkOperationResult:
    def __init__(
        self,
        total: int = 0,
        documents_processed: int = 0,
        attachments_processed: int = 0,
        counters_processed: int = 0,
        time_series_processed: int = 0,
        query: Optional[str] = None,
        details: Optional[list] = None,
    ):
        self.total = total
        self.documents_processed = documents_processed
        self.attachments_processed = attachments_processed
        self.counters_processed = counters_processed
        self.time_series_processed = time_series_processed
        self.query = query
        self.details = details if details is not None else []

    @property
    def message(self) -> str:
        return f"Processed {self.total:,} items."

    @classmethod
    def from_json(cls, json_dict: dict) -> "BulkOperationResult":
        if json_dict is None:
            return cls()
        return cls(
            total=json_dict.get("Total", 0),
            documents_processed=json_dict.get("DocumentsProcessed", 0),
            attachments_processed=json_dict.get("AttachmentsProcessed", 0),
            counters_processed=json_dict.get("CountersProcessed", 0),
            time_series_processed=json_dict.get("TimeSeriesProcessed", 0),
            query=json_dict.get("Query"),
            details=json_dict.get("Details", []),
        )


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
        for _ in range(10):
            command = self._get_operation_state_command(self.__conventions, self.__key, self.node_tag)
            self.__request_executor.execute_command(command)
            if command.result is not None:
                return command.result
            time.sleep(0.5)
        raise InvalidOperationException(
            f"Could not fetch state of operation '{self.__key}' from node '{self.node_tag}'."
        )

    def _get_operation_state_command(
        self, conventions: "DocumentConventions", key: int, node_tag: str = None
    ) -> RavenCommand[dict]:
        return GetOperationStateOperation.GetOperationStateCommand(self.__key, node_tag)

    def wait_for_completion(self, timeout: Optional[timedelta] = None) -> Optional[BulkOperationResult]:
        deadline = time.monotonic() + timeout.total_seconds() if timeout is not None else None
        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(
                    f"Operation {self.__key} did not complete within the specified timeout of {timeout}."
                )

            status = self.fetch_operations_status()
            operation_status = status.get("Status")

            if operation_status == "Completed":
                result = status.get("Result")
                if result is not None:
                    return BulkOperationResult.from_json(result)
                return None
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

            sleep_secs = 0.5
            if deadline is not None:
                sleep_secs = min(sleep_secs, max(0.0, deadline - time.monotonic()))
            time.sleep(sleep_secs)
