from __future__ import annotations

import _queue
import json
from concurrent.futures import Future
from queue import Queue
from threading import Lock, Semaphore
from typing import Optional, TYPE_CHECKING

import requests
from ravendb import constants
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.http.server_node import ServerNode
from ravendb.http.raven_command import RavenCommand
from ravendb.documents.operations.misc import GetOperationStateOperation
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.documents.commands.batches import CommandType
from ravendb.documents.commands.bulkinsert import GetNextOperationIdCommand, KillOperationCommand
from ravendb.exceptions.documents.bulkinsert import BulkInsertAbortedException
from ravendb.documents.identity.hilo import GenerateEntityIdOnTheClient

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


class BulkInsertOperation:
    class _BufferExposer:
        def __init__(self):
            self._data = {}
            self._ongoing_operation = Future()  # todo: is there any reason to use Futures? (look at error handling)
            self._yield_buffer_semaphore = Semaphore(1)
            self._buffers_to_flush_queue = Queue()
            self.output_stream = Future()
            self.running = True

        def enqueue_buffer_for_flush(self, buffer: bytearray):
            self._buffers_to_flush_queue.put(buffer)

        # todo: blocking semaphore acquired and released on enter and exit from bulk insert operation context manager
        def send_data(self):
            while self.running:
                try:
                    buffer_to_flush = self._buffers_to_flush_queue.get(timeout=0.05)  # todo: adjust this pooling time
                    yield buffer_to_flush
                except Exception as e:
                    if not isinstance(e, _queue.Empty) or self.is_operation_finished():
                        break
                    continue  # expected Empty exception coming from queue, operation isn't finished yet

        def is_operation_finished(self) -> bool:
            return self._ongoing_operation.done()

        def finish_operation(self):
            self._ongoing_operation.set_result(None)

        def error_on_processing_request(self, exception: Exception):
            self._ongoing_operation.set_exception(exception)

        def error_on_request_start(self, exception: Exception):
            self.output_stream.set_exception(exception)

    class _BulkInsertCommand(RavenCommand[requests.Response]):
        def __init__(self, key: int, buffer_exposer: BulkInsertOperation._BufferExposer, node_tag: str):
            super().__init__(requests.Response)
            self._buffer_exposer = buffer_exposer
            self._key = key
            self._selected_node_tag = node_tag
            self.use_compression = False

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "POST",
                f"{node.url}/databases/{node.database}/bulk_insert?id={self._key}",
                data=self._buffer_exposer.send_data(),
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            raise NotImplementedError("Not Implemented")

        def is_read_request(self) -> bool:
            return False

        def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
            try:
                return super().send(session, request)
            except Exception as e:
                self._buffer_exposer.error_on_request_start(e)

    def __init__(self, database: str = None, store: "DocumentStore" = None):
        self.use_compression = False

        self._ongoing_bulk_insert_execute_task: Optional[Future] = None
        self._first = True
        self._in_progress_command: Optional[CommandType] = None
        self._operation_id = -1
        self._node_tag = None
        self._concurrent_check = 0
        self._concurrent_check_lock = Lock()

        self._thread_pool_executor = store.thread_pool_executor
        self._conventions = store.conventions
        if not database or database.isspace():
            self._throw_no_database()
        self._request_executor = store.get_request_executor(database)

        self._enqueue_current_buffer_async = Future()
        self._enqueue_current_buffer_async.set_result(None)

        self._max_size_in_buffer = 1024 * 1024

        self._current_data_buffer = bytearray()

        # todo: self._time_series_batch_size = self._conventions.time_series_batch_size
        self._buffer_exposer = BulkInsertOperation._BufferExposer()

        self._generate_entity_id_on_the_client = GenerateEntityIdOnTheClient(
            self._request_executor.conventions,
            lambda entity: self._request_executor.conventions.generate_document_id(database, entity),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_previous_command_if_needed()

        flush_ex = None

        if self._buffer_exposer.is_operation_finished():
            return

        # process the leftovers and finish the stream
        if self._current_data_buffer:
            try:
                self._write_misc("]")
                self._enqueue_current_buffer_async.result()  # wait for enqueue
                buffer = self._current_data_buffer
                self._buffer_exposer.enqueue_buffer_for_flush(buffer)
            except Exception as e:
                flush_ex = e

        self._buffer_exposer.finish_operation()

        if self._operation_id == -1:
            # closing without calling a single store
            return

        if self._ongoing_bulk_insert_execute_task is not None:
            try:
                self._ongoing_bulk_insert_execute_task.result()
            except Exception as e:
                self._throw_bulk_insert_aborted(e, flush_ex)

    def _throw_bulk_insert_aborted(self, e: Exception, flush_ex: Optional[Exception]):
        error_from_server = None
        try:
            error_from_server = self._get_exception_from_operation()
        except Exception:
            pass  # server is probably down, will propagate the original exception

        if error_from_server is not None:
            raise error_from_server

        raise BulkInsertAbortedException("Failed to execute bulk insert", e or flush_ex)

    def _throw_no_database(self):
        raise RuntimeError(
            "Cannot start bulk insert operation without specifying a name of a database to operate on. "
            "Database name can be passed as an argument when bulk insert is being created or "
            "default database can be defined using 'DocumentStore.database' property."
        )

    def _get_bulk_insert_operation_id(self):
        if self._operation_id != -1:
            return

        bulk_insert_get_id_request = GetNextOperationIdCommand()
        self._request_executor.execute_command(bulk_insert_get_id_request)
        self._operation_id = bulk_insert_get_id_request.result
        self._node_tag = bulk_insert_get_id_request._node_tag

    def _fill_metadata_if_needed(self, entity: object, metadata: MetadataAsDictionary):
        # add collection name to metadata if needed
        if constants.Documents.Metadata.COLLECTION not in metadata:
            collection = self._request_executor.conventions.get_collection_name(entity)
            if collection is not None:
                metadata[constants.Documents.Metadata.COLLECTION] = collection

        # add type path to metadata if needed
        if constants.Documents.Metadata.RAVEN_PYTHON_TYPE not in metadata:
            python_type = self._request_executor.conventions.get_python_class_name(entity.__class__)
            if python_type is not None:
                metadata[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

    def store(self, entity: object, metadata: Optional[MetadataAsDictionary] = None) -> str:
        key = (
            self._get_id(entity)
            if metadata is None or constants.Documents.Metadata.ID not in metadata
            else metadata[constants.Documents.Metadata.ID]
        )

        self.store_as(entity, key, metadata or MetadataAsDictionary())
        return key

    def store_as(
        self, entity: object, key: str, metadata: Optional[MetadataAsDictionary] = MetadataAsDictionary()
    ) -> None:
        try:
            self._concurrency_check()
            self._verify_valid_key(key)
            self._ensure_ongoing_operation()

            self._fill_metadata_if_needed(entity, metadata)
            self._end_previous_command_if_needed()  # counters & time series commands shall end before pushing docs

            try:
                if not self._first:
                    self._write_comma()

                self._first = False
                self._in_progress_command = CommandType.NONE

                self._write_misc('{"Id":"')
                self._write_key(key)
                self._write_misc('","Type":"PUT","Document":')

                # self._flush_if_needed() # why?

                self._write_document(entity, metadata)
                self._write_misc("}")
            except Exception as e:
                self._handle_errors(key, e)
        finally:
            with self._concurrent_check_lock:
                self._concurrent_check = 0

    def _handle_errors(self, document_id: str, e: Exception) -> None:
        error = self._get_exception_from_operation()
        if error is not None:
            raise error

        self._throw_on_unavailable_stream(document_id, e)

    def _concurrency_check(self):
        with self._concurrent_check_lock:
            if not self._concurrent_check == 0:
                raise RuntimeError("Bulk Insert store methods cannot be executed concurrently.")
            self._concurrent_check = 1

        def __return_func():
            with self._concurrent_check_lock:
                if self._concurrent_check == 1:
                    self._concurrent_check = 0

        return __return_func

    def _flush_if_needed(self) -> None:
        if len(self._current_data_buffer) > self._max_size_in_buffer or self._enqueue_current_buffer_async.done():
            self._enqueue_current_buffer_async.result()

            buffer = self._current_data_buffer
            self._current_data_buffer.clear()

            # todo: check if it's better to create a new bytearray of max size instead of clearing it (possible dealloc)

            def __enqueue_buffer_for_flush():
                self._buffer_exposer.enqueue_buffer_for_flush(buffer)

            self._enqueue_current_buffer_async = self._thread_pool_executor.submit(__enqueue_buffer_for_flush)

    def _end_previous_command_if_needed(self) -> None:
        if self._in_progress_command == CommandType.COUNTERS:
            pass  # todo: counters
        elif self._in_progress_command == CommandType.TIME_SERIES:
            pass  # todo: time series

    def _write_key(self, input_string: str) -> None:
        for i in range(len(input_string)):
            c = input_string[i]
            if '"' == c:
                if i == 0 or input_string[i - 1] != "\\":
                    self._current_data_buffer += bytearray("\\", encoding="utf-8")

            self._current_data_buffer += bytearray(c, encoding="utf-8")

    def _write_comma(self) -> None:
        self._current_data_buffer += bytearray(",", encoding="utf-8")

    def _write_misc(self, data: str) -> None:
        self._current_data_buffer += bytearray(data, encoding="utf-8")

    def _write_document(self, entity: object, metadata: MetadataAsDictionary):
        document_info = DocumentInfo(metadata_instance=metadata)
        json_dict = EntityToJson.convert_entity_to_json_internal_static(entity, self._conventions, document_info, True)
        self._current_data_buffer += bytearray(json.dumps(json_dict), encoding="utf-8")

    def _ensure_ongoing_operation(self) -> None:
        if self._ongoing_bulk_insert_execute_task is None:
            self._get_bulk_insert_operation_id()
            self._start_executing_bulk_insert_command()

        if (
            self._ongoing_bulk_insert_execute_task.done() and self._ongoing_bulk_insert_execute_task.exception()
        ):  # todo: check if isCompletedExceptionally returns false if task isn't finished
            try:
                self._ongoing_bulk_insert_execute_task.result()
            except Exception as e:
                self._throw_bulk_insert_aborted(e, None)

    @staticmethod
    def _verify_valid_key(key: str) -> None:
        if not key or key.isspace():
            raise ValueError("Document id must have a non empty value")

        if key.endswith("|"):
            raise RuntimeError(f"Document ids cannot end with '|', but was called with {key}")

    def _get_exception_from_operation(self) -> Optional[BulkInsertAbortedException]:
        state_request = GetOperationStateOperation.GetOperationStateCommand(self._operation_id, self._node_tag)
        self._request_executor.execute_command(state_request)

        if "Faulted" != state_request.result["Status"]:
            return None

        result = state_request.result["Result"]

        if result["$type"].starts_with("Raven.Client.Documents.Operations.OperationExceptionResult"):
            return BulkInsertAbortedException(result["Error"])

        return None

    def _start_executing_bulk_insert_command(self) -> None:
        try:
            bulk_command = BulkInsertOperation._BulkInsertCommand(
                self._operation_id, self._buffer_exposer, self._node_tag
            )
            bulk_command.use_compression = self.use_compression

            def __execute_bulk_insert_raven_command():
                self._request_executor.execute_command(bulk_command)

            self._ongoing_bulk_insert_execute_task = self._thread_pool_executor.submit(
                __execute_bulk_insert_raven_command
            )
            self._current_data_buffer += bytearray("[", encoding="utf-8")

        except Exception as e:
            raise RavenException("Unable to open bulk insert stream", e)

    def _throw_on_unavailable_stream(self, key: str, inner_ex: Exception) -> None:
        self._buffer_exposer.error_on_processing_request(
            BulkInsertAbortedException(f"Write to stream failed at document with id {key}", inner_ex)
        )

    def abort(self) -> None:
        if self._operation_id == -1:
            return  # nothing was done, nothing to kill

        self._get_bulk_insert_operation_id()

        try:
            self._request_executor.execute_command(KillOperationCommand(self._operation_id, self._node_tag))
        except RavenException as e:
            raise BulkInsertAbortedException(
                "Unable to kill this bulk insert operation, because it was not found on the server", e
            )

    def _get_id(self, entity: object) -> str:
        success, key = self._generate_entity_id_on_the_client.try_get_id_from_instance(entity)
        if success:
            return key

        key = self._generate_entity_id_on_the_client.generate_document_key_for_storage(entity)

        self._generate_entity_id_on_the_client.try_set_identity(entity, key)
        return key

    # todo: attachments_for
    # todo: time_series_for
    # todo: CountersBulkInsert
    # todo: CountersBulkInsertOperation
    # todo: TimeSeriesBulkInsertBase
    # todo: TimeSeriesBulkInsert
    # todo: TypedTimeSeriesBulkInsert
    # todo: AttachmentsBulkInsert
    # todo: AttachmentsBulkInsertOperation
