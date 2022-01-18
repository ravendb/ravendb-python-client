from __future__ import annotations

import datetime
import json
import os
import time
import uuid
from typing import Union, Callable, TYPE_CHECKING, Optional, Dict, List, Any

from pyravendb import constants
from pyravendb.custom_exceptions import exceptions
from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from pyravendb.data.operation import AttachmentType
from pyravendb.data.timeseries import TimeSeriesRange
import pyravendb.documents
from pyravendb.documents.indexes import AbstractCommonApiForIndexes
from pyravendb.documents.operations.attachments import (
    GetAttachmentOperation,
    AttachmentName,
    AttachmentRequest,
    GetAttachmentsOperation,
)
from pyravendb.documents.session.cluster_transaction_operation import ClusterTransactionOperations
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.document_query import Query
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.loaders import MultiLoaderWithInclude, LoaderWithInclude
from pyravendb.documents.session.operations.lazy import LazySessionOperations
from pyravendb.documents.session.operations.lazy.lazy import LazyLoadOperation
from pyravendb.documents.session.operations.operations import MultiGetOperation, LoadStartingWithOperation
from pyravendb.documents.session import (
    SessionOptions,
    ResponseTimeInformation,
    JavaScriptArray,
    JavaScriptMap,
    DocumentsChanges,
)
from pyravendb.json.metadata_as_dictionary import MetadataAsDictionary
from pyravendb.loaders.include_builder import IncludeBuilder
from pyravendb.raven_operations.load_operation import LoadOperation
from pyravendb.tools.utils import Utils
from pyravendb.documents.commands.batches import (
    PatchCommandData,
    CommandType,
    DeleteCommandData,
    PutAttachmentCommandData,
    DeleteAttachmentCommandData,
    CopyAttachmentCommandData,
    MoveAttachmentCommandData,
)
from pyravendb.documents.commands import HeadDocumentCommand, GetDocumentsCommand, HeadAttachmentCommand
from pyravendb.documents.commands.multi_get import GetRequest
from pyravendb.documents.operations import BatchOperation, PatchRequest

if TYPE_CHECKING:
    from pyravendb.data.document_conventions import DocumentConventions
    from pyravendb.documents.operations.lazy.lazy_operation import LazyOperation
    from pyravendb.documents.session.cluster_transaction_operation import ClusterTransactionOperationsBase


class DocumentSession(InMemoryDocumentSessionOperations):
    def __init__(self, store: pyravendb.documents.DocumentStore, key: uuid.UUID, options: SessionOptions):
        super().__init__(store, key, options)

        self.__advanced: DocumentSession._Advanced = DocumentSession._Advanced(self)
        self.__document_query_generator: DocumentSession._DocumentQueryGenerator = (
            DocumentSession._DocumentQueryGenerator(self)
        )
        self.__cluster_transaction: Union[None, ClusterTransactionOperations] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def advanced(self):
        return self.__advanced

    @property
    def _lazily(self):
        return LazySessionOperations(self)

    @property
    def cluster_session(self) -> ClusterTransactionOperationsBase:
        if self.__cluster_transaction is None:
            self.__cluster_transaction = ClusterTransactionOperations(self)
        return self.__cluster_transaction

    @property
    def has_cluster_session(self) -> bool:
        return self.__cluster_transaction is not None

    def save_changes(self) -> None:
        save_changes_operation = BatchOperation(self)
        command = save_changes_operation.create_request()
        if command:
            with command:
                if command is None:
                    return

                if self.no_tracking:
                    raise RuntimeError("Cannot execute save_changes when entity tracking is disabled.")

                # todo: rebuild request executor - WIP
                self.request_executor.execute_command(command, self._session_info)
                self.update_session_after_save_changes(command.result)
                save_changes_operation.set_result(command.result)

    def _clear_cluster_session(self) -> None:
        if not self._has_cluster_session():
            return
        self.get_cluster_session().clear()

    def _generate_id(self, entity: object) -> str:
        return self.conventions.generate_document_id(self.database_name, entity)

    def _execute_lazy_operations_single_step(
        self, response_time_information: ResponseTimeInformation, get_requests: List[GetRequest], sw: datetime.datetime
    ) -> bool:
        multi_get_operation = MultiGetOperation(self)
        with multi_get_operation.create_request(get_requests) as multi_get_command:
            self.request_executor.execute_command(multi_get_command, self._session_info)

            responses = multi_get_command.result

            if not multi_get_command.aggressively_cached:
                self.increment_requests_count()

            for i in range(len(self._pending_lazy_operations)):
                response = responses[i]

                temp_req_time = response.headers.get(constants.Headers.REQUEST_TIME)
                response.elapsed = datetime.datetime.now() - sw
                total_time = temp_req_time if temp_req_time is not None else 0

                time_item = ResponseTimeInformation.ResponseTimeItem(get_requests[i].url_and_query, total_time)

                response_time_information.duration_breakdown.append(time_item)

                if response.request_has_errors:
                    raise RuntimeError(
                        f"Got an error from server, status code: {response.status_code}{os.linesep}{response.result}"
                    )

                self._pending_lazy_operations[i].handle_response(response)
                if self._pending_lazy_operations[i].is_requires_retry:
                    return True

            return False

    def include(self, path: str) -> LoaderWithInclude:
        return MultiLoaderWithInclude(self).include(path)

    def add_lazy_operation(
        self, object_type, operation: LazyOperation, on_eval: Union[None, Callable[[Any], None]]
    ) -> pyravendb.documents.Lazy:
        self._pending_lazy_operations.append(operation)

        def __supplier():
            self.advanced.eagerly.execute_all_pending_lazy_operations()
            return self._get_operation_result(object_type, operation.result)

        lazy_value = pyravendb.documents.Lazy(__supplier)

        if on_eval is not None:
            self._on_evaluate_lazy[operation] = lambda the_result: on_eval(
                self._get_operation_result(object_type, the_result)
            )

        return lazy_value

    def add_lazy_count_operation(self, operation: LazyOperation) -> pyravendb.documents.Lazy:
        self._pending_lazy_operations.append(operation)

        def __supplier():
            self.advanced.eagerly.execute_all_pending_lazy_operations()
            return operation.query_result.total_results

        return pyravendb.documents.Lazy(__supplier)

    def lazy_load_internal(
        self, object_type: type, keys: List[str], includes: List[str], on_eval: Callable[[Dict[str, object]], None]
    ) -> pyravendb.documents.Lazy:
        if self.check_if_id_already_included(keys, includes):
            return pyravendb.documents.Lazy(lambda: self.load(keys, object_type))

        load_operation = LoadOperation(self).by_keys(keys).with_includes(includes)
        lazy_op = LazyLoadOperation(object_type, self, load_operation).by_keys(*keys).with_includes(*includes)
        return self.add_lazy_operation(dict, lazy_op, on_eval)

    def load(
        self,
        key_or_keys: Union[List[str], str],
        object_type: Optional[type] = None,
        includes: Callable[[IncludeBuilder], None] = None,
        nested_object_types: Dict[str, type] = None,
    ) -> Union[Dict[str, object], object]:
        if key_or_keys is None:
            return None  # todo: return default value of object_type, not always None
        if includes is None:
            load_operation = LoadOperation(self)
            self.__load_internal_stream(
                [key_or_keys] if isinstance(key_or_keys, str) else key_or_keys, load_operation, None
            )
            result = load_operation.get_documents(object_type)
            return result.popitem()[1] if len(result) == 1 else result if result else None
        include_builder = IncludeBuilder(self.conventions)
        includes(include_builder)

        time_series_includes = (
            [include_builder.time_series_to_include] if include_builder.time_series_to_include is not None else None
        )

        compare_exchange_values_to_include = (
            include_builder.compare_exchange_values_to_include
            if include_builder.compare_exchange_values_to_include is not None
            else None
        )

        result = self._load_internal(
            object_type,
            [key_or_keys] if isinstance(key_or_keys, str) else key_or_keys,
            include_builder.documents_to_include if include_builder.documents_to_include else None,
            include_builder.counters_to_include if include_builder.counters_to_include else None,
            include_builder.is_all_counters,
            time_series_includes,
            compare_exchange_values_to_include,
        )

        return result.popitem()[1] if len(result) == 1 else result

    def _load_internal(
        self,
        object_type: type,
        keys: List[str],
        includes: List[str],
        counter_includes: List[str] = None,
        include_all_counters: bool = False,
        time_series_includes: List[TimeSeriesRange] = None,
        compare_exchange_value_includes: List[str] = None,
    ) -> Dict[str, object]:
        if not keys:
            raise ValueError("Keys cannot be None")
        load_operation = LoadOperation(self)
        load_operation.by_keys(keys)
        load_operation.with_includes(includes)

        if include_all_counters:
            load_operation.with_all_counters()
        else:
            load_operation.with_counters(counter_includes)

        load_operation.with_time_series(time_series_includes)
        load_operation.with_compare_exchange(compare_exchange_value_includes)

        command = load_operation.create_request()
        if command is not None:
            self.request_executor.execute_command(command, self._session_info)
            load_operation.set_result(command.result)

        return load_operation.get_documents(object_type)

    def __load_internal_stream(self, keys: List[str], operation: LoadOperation, stream: bytes) -> None:
        operation.by_keys(keys)

        command = operation.create_request()

        if command:
            self._request_executor.execute_command(command, self._session_info)

            if stream:
                try:
                    result = command.result
                    stream_to_dict = json.loads(stream.decode("utf-8"))
                    result.__dict__.update(stream_to_dict)
                except IOError as e:
                    raise RuntimeError(f"Unable to serialize returned value into stream {e.args[0]}", e)
            else:
                operation.set_result(command.result)

    def load_starting_with(
        self,
        object_type: type,
        id_prefix: str,
        matches: Optional[str] = None,
        start: Optional[int] = None,
        page_size: Optional[int] = None,
        exclude: Optional[str] = None,
        start_after: Optional[str] = None,
    ):
        load_starting_with_operation = LoadStartingWithOperation(self)
        self.__load_starting_with_internal(
            id_prefix, load_starting_with_operation, None, matches, start, page_size, exclude, start_after
        )
        return load_starting_with_operation.get_documents(object_type)

    def __load_starting_with_internal(
        self,
        id_prefix: str,
        operation: LoadStartingWithOperation,
        stream,
        matches: str,
        start: int,
        page_size: int,
        exclude: str,
        start_after: str,
    ):
        operation.with_start_with(id_prefix, matches, start, page_size, exclude, start_after)
        command = operation.create_request()
        if command:
            self._request_executor.execute_command(command, self._session_info)
            if stream:
                pass  # todo: stream
            else:
                operation.set_result(command.result)

        return command

    def query(
        self,
        object_type: type = None,
        collection_name: Optional[str] = None,
        index_name: Optional[str] = None,
        **kwargs,
    ) -> Query:
        if collection_name is not None and index_name is not None:
            raise ValueError("Pass either collection_name or index_name")
        return self.__document_query_generator.document_query(object_type, index_name, collection_name, **kwargs)

    class _EagerSessionOperations:
        def __init__(self, session: DocumentSession):
            self.__session = session

        def execute_all_pending_lazy_operations(self) -> ResponseTimeInformation:
            operations_to_remove = []

            requests = []

            for lazy_operation in self.__session._pending_lazy_operations:
                req = lazy_operation.create_request()
                if req is None:
                    operations_to_remove.append(lazy_operation)
                    continue
                requests.append(req)

            for op in operations_to_remove:
                self.__session._pending_lazy_operations.remove(op)

            if not requests:
                return ResponseTimeInformation()

            sw = datetime.datetime.now()
            response_time_duration = ResponseTimeInformation()
            while self.__session._execute_lazy_operations_single_step(response_time_duration, requests, sw):
                time.sleep(0.1)
            response_time_duration.compute_server_total()

            for pending_lazy_operation in self.__session._pending_lazy_operations:
                value = self.__session._on_evaluate_lazy.get(pending_lazy_operation)
                if value is not None:
                    value(pending_lazy_operation.result)

            elapsed = datetime.datetime.now() - sw
            response_time_duration.total_client_duration = elapsed

            self.__session._pending_lazy_operations.clear()

            return response_time_duration

    class _Advanced:
        def __init__(self, session: DocumentSession):
            self.__session = session
            self.__vals_count = 0
            self.__custom_count = 0
            self.__attachment = None

            self.eagerly = DocumentSession._EagerSessionOperations(session)

        @property
        def attachment(self):
            if not self.__attachment:
                self.__attachment = self._Attachment(self.__session)
            return self.__attachment

        def refresh(self, entity: object) -> object:
            document_info = self.__session.documents_by_entity.get(entity)
            if document_info is None:
                raise ValueError("Cannot refresh a transient instance")
            self.__session.increment_requests_count()

            command = GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsByIdsCommandOptions([document_info.key], None, False)
            )
            self.__session.request_executor.execute_command(command, self.__session._session_info)
            entity = self.__session._refresh_internal(entity, command, document_info)
            return entity

        def raw_query(self, query: str, query_parameters: Optional[dict] = None, **kwargs):  # -> RawDocumentQuery:
            self._query = Query(self.__session)(**kwargs)
            return self._query.raw_query(query)

        @property
        def lazily(self) -> LazySessionOperations:
            return self.__session._lazily

        def graph_query(self, object_type: type, query: str):  # -> GraphDocumentQuery:
            pass

        def what_changed(self) -> Dict[str, List[DocumentsChanges]]:
            return self.__session._what_changed()

        def exists(self, key: str) -> bool:
            if key is None:
                raise ValueError("Key cannot be None")
            if key in self.__session._known_missing_ids:
                return False

            if self.__session.documents_by_id.get(key) is not None:
                return True

            command = HeadDocumentCommand(key, None)
            self.__session.request_executor.execute_command(command, self.__session._session_info)
            return command.result is not None

        def __load_starting_with_internal(
            self,
            id_prefix: str,
            operation: LoadStartingWithOperation,
            stream: Union[None, bytes],
            matches: str,
            start: int,
            page_size: int,
            exclude: str,
            start_after: str,
        ) -> GetDocumentsCommand:
            operation.with_start_with(id_prefix, matches, start, page_size, exclude, start_after)
            command = operation.create_request()
            if command is not None:
                self.__session.request_executor.execute(command, self.__session._session_info)
                if stream:
                    try:
                        result = command.result
                        stream_to_dict = json.loads(stream.decode("utf-8"))
                        result.__dict__.update(stream_to_dict)
                    except IOError as e:
                        raise RuntimeError(f"Unable to serialize returned value into stream {e.args[0]}", e)
                else:
                    operation.set_result(command.result)
            return command

        def load_starting_with(
            self,
            object_type: type,
            id_prefix: str,
            matches: str,
            start: int,
            page_size: int,
            exclude: str,
            start_after: str,
        ) -> object:
            load_starting_with_operation = LoadStartingWithOperation(self.__session)
            self.__load_starting_with_internal(
                id_prefix, load_starting_with_operation, None, matches, start, page_size, exclude, start_after
            )

        def load_starting_with_into_stream(
            self,
            id_prefix: str,
            output: bytes,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            exclude: str = None,
            start_after: str = None,
        ):
            if not output:
                raise ValueError("Output cannot be None")
            if not id_prefix:
                raise ValueError("Id prefix cannot be None")
            self.__load_starting_with_internal(
                id_prefix,
                LoadStartingWithOperation(self.__session),
                output,
                matches,
                start,
                page_size,
                exclude,
                start_after,
            )

        def load_into_stream(self, keys: List[str], output: bytes) -> None:
            if keys is None:
                raise ValueError("Keys cannot be None")

            self.__session.__load_internal_stream(keys, LoadOperation(self.__session), output)

        def __try_merge_patches(self, key: str, patch_request: PatchRequest) -> bool:
            command = self.__session.deferred_commands_map.get(
                pyravendb.documents.IdTypeAndName.create(key, CommandType.PATCH, None)
            )
            if command is None:
                return False

            self.__session.deferred_commands.remove(command)
            # We'll overwrite the deferredCommandsMap when calling Defer
            # No need to call deferredCommandsMap.remove((id, CommandType.PATCH, null));

            old_patch: PatchCommandData = command
            new_script = old_patch.patch.script + "\n" + patch_request.script
            new_vals = dict(old_patch.patch.values)

            for key, value in patch_request.values.items():
                new_vals[key] = value

            new_patch_request = PatchRequest()
            new_patch_request.script = new_script
            new_patch_request.values = new_vals

            self.__session.defer(PatchCommandData(key, None, new_patch_request, None))
            return True

        def increment(self, key_or_entity: Union[object, str], path: str, value_to_add: object) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.__session.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))
            patch_request = PatchRequest()
            variable = f"this.{path}"
            value = f"args.val_{self.__vals_count}"
            patch_request.script = f"{variable} = {variable} ? {variable} + {value} : {value} ;"
            patch_request.values = {f"val_{self.__vals_count}": value_to_add}
            self.__vals_count += 1
            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.__session.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch(self, key_or_entity: Union[object, str], path: str, value: object) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.__session.get_metadata_for(key_or_entity)
                key_or_entity = metadata[constants.Documents.Metadata.ID]

            patch_request = PatchRequest()
            patch_request.script = f"this.{path} = args.val_ {self.__vals_count};"
            patch_request.values = {f"val_{self.__vals_count}": value}

            self.__vals_count += 1

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.__session.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch_array(
            self, key_or_entity: Union[object, str], path_to_array: str, array_adder: Callable[[JavaScriptArray], None]
        ) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.__session.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))

            script_array = JavaScriptArray(self.__custom_count, path_to_array)
            self.__custom_count += 1

            array_adder(script_array)

            patch_request = PatchRequest()
            patch_request.script = script_array.script
            patch_request.values = script_array.parameters

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.__session.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch_object(
            self, key_or_entity: Union[object, str], path_to_object: str, dictionary_adder: Callable[[dict], None]
        ) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.__session.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))

            script_map = JavaScriptMap(self.__custom_count, path_to_object)
            self.__custom_count += 1

            patch_request = PatchRequest()
            patch_request.script = script_map.script
            patch_request.values = script_map.parameters

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.__session.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def add_or_patch(self, key: str, entity: object, path_to_object: str, value: object) -> None:
            patch_request = PatchRequest()
            patch_request.script = f"this.{path_to_object} = args.val_{self.__vals_count}"
            patch_request.values = {f"val_{self.__vals_count}": value}

            collection_name = self.__session.request_executor.conventions.get_collection_name(entity)
            python_type = self.__session.request_executor.conventions.get_python_class_name(type(entity))

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self.__session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__vals_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.__session.defer(patch_command_data)

        def add_or_patch_array(
            self, key: str, entity: object, path_to_array: str, list_adder: Callable[[JavaScriptArray], None]
        ) -> None:
            script_array = JavaScriptArray(self.__custom_count, path_to_array)
            self.__custom_count += 1

            list_adder(script_array)

            patch_request = PatchRequest()
            patch_request.script = script_array.script
            patch_request.values = script_array.parameters

            collection_name = self.__session.request_executor.conventions.get_collection_name(entity)
            python_type = self.__session.request_executor.conventions.get_python_class_name(type(entity))

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self.__session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__vals_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.__session.defer(patch_command_data)

        def add_or_increment(self, key: str, entity: object, path_to_object: str, val_to_add: object) -> None:
            variable = f"this.{path_to_object}"
            value = f"args.val_{self.__vals_count}"

            patch_request = PatchRequest()
            patch_request.script = f"{variable} = {variable} ? {variable} + {value} : {value}"
            patch_request.values = {f"val_{self.__vals_count}": val_to_add}

            collection_name = self.__session.request_executor.conventions.get_collection_name(entity)
            python_type = self.__session.request_executor.conventions.find_python_class_name(entity.__class__)

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self.__session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__vals_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.__session.defer(patch_command_data)

        def stream(
            self,
            query_or_raw_query,  # : Union[RawDocumentQuery, DocumentQuery],
            stream_query_stats,  # : Optional[StreamQueryStatistics] = None,
        ) -> iter:
            pass

        def stream_starting_with(
            self,
            object_type: type,
            starts_with: str,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            starting_after: str = None,
        ) -> iter:
            pass

        def stream_into(self):  # query: Union[DocumentQuery, RawDocumentQuery], output: iter):
            pass

        def conditional_load(self, object_type: type, key: str, change_vector: str):
            pass

            # todo: stream, query and fors like timeseriesrollupfor, conditional load

        class _Attachment:
            def __init__(self, session: DocumentSession):
                self.__session = session
                self.__store = session._document_store
                self.__defer_commands = session.deferred_commands

            def _command_exists(self, document_id):
                for command in self.__defer_commands:
                    if not isinstance(
                        command,
                        (
                            PutAttachmentCommandData,
                            DeleteAttachmentCommandData,
                            DeleteCommandData,
                        ),
                    ):
                        continue

                    if command.key == document_id:
                        return command

            def throw_not_in_session(self, entity):
                raise ValueError(
                    repr(entity) + " is not associated with the session, cannot add attachment to it. "
                    "Use document Id instead or track the entity in the session."
                )

            def exists(self, document_id: str, name: str) -> bool:
                command = HeadAttachmentCommand(document_id, name, None)
                self.__session.increment_requests_count()
                self.__session.request_executor.execute_command(command, self.__session._session_info)
                return command.result is not None

            def store(self, entity_or_document_id, name, stream, content_type=None, change_vector=None):
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.key

                if not entity_or_document_id:
                    raise ValueError(entity_or_document_id)
                if not name:
                    raise ValueError(name)

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.DELETE, None)
                ) in self.__session.deferred_commands_map:
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "store", "delete")

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_PUT, name)
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "store", "create")

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_DELETE, name)
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "store", "delete")

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_MOVE, name)
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "store", "rename")

                document_info = self.__session.documents_by_id.get_value(entity_or_document_id)
                if document_info and document_info.entity in self.__session.deleted_entities:
                    raise exceptions.InvalidOperationException(
                        "Can't store attachment "
                        + name
                        + " of document"
                        + entity_or_document_id
                        + ", the document was already deleted in this session."
                    )

                self.__session.defer(
                    PutAttachmentCommandData(entity_or_document_id, name, stream, content_type, change_vector)
                )

            def delete(self, entity_or_document_id, name):
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.metadata["@id"]

                if not entity_or_document_id:
                    raise ValueError(entity_or_document_id)
                if not name:
                    raise ValueError(name)

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.DELETE, None)
                    in self.__session.deferred_commands_map
                    or pyravendb.documents.IdTypeAndName.create(
                        entity_or_document_id, CommandType.ATTACHMENT_DELETE, name
                    )
                    in self.__session.deferred_commands_map
                ):
                    return

                document_info = self.__session.documents_by_id.get_value(entity_or_document_id)
                if document_info and document_info.entity in self.__session.deleted_entities:
                    return

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_PUT, name)
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "delete", "create")

                if (
                    pyravendb.documents.IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_MOVE, name)
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "delete", "rename")

                self.__session.defer(DeleteAttachmentCommandData(entity_or_document_id, name, None))

            def get(
                self,
                entity_or_document_id: str = None,
                name: str = None,
                # att_requests: Optional[List[AttachmentRequest]] = None,
                # todo: fetching multiple attachments with single command
            ):
                # if att_requests is not None:
                #     if entity_or_document_id or name:
                #         raise ValueError("Specify either <att_requests> or <entity/document_id, name>")
                #     operation = GetAttachmentsOperation(att_requests, AttachmentType.document)
                # else:
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.metadata["@id"]

                operation = GetAttachmentOperation(entity_or_document_id, name, AttachmentType.document, None)
                return self.__store.operations.send(operation)

            def copy(
                self,
                entity_or_document_id: Union[object, str],
                source_name: str,
                destination_entity_or_document_id: object,
                destination_name: str,
            ) -> None:
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.key

                if not isinstance(destination_entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(destination_entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(destination_entity_or_document_id)
                    destination_entity_or_document_id = entity.key
                if entity_or_document_id is None or entity_or_document_id.isspace():
                    raise ValueError("Source entity id is required")

                if source_name is None or source_name.isspace():
                    raise ValueError("Source name is required")
                if destination_entity_or_document_id is None or destination_entity_or_document_id.isspace():
                    raise ValueError("Destination entity id is required")
                if destination_name is None or destination_name.isspace():
                    raise ValueError("Destination name is required")

                if (
                    entity_or_document_id.lower() == destination_entity_or_document_id.lower()
                    and source_name == destination_name
                ):
                    return  # no-op

                source_document = self.__session.documents_by_id.get_value(entity_or_document_id)
                if entity_or_document_id and source_document.entity in self.__session.deleted_entities:
                    self.__throw_document_already_deleted(
                        entity_or_document_id,
                        source_name,
                        "copy",
                        destination_entity_or_document_id,
                        entity_or_document_id,
                    )

                destination_document = self.__session.documents_by_id.get_value(destination_entity_or_document_id)
                if destination_document and destination_document.entity in self.__session.deleted_entities:
                    self.__throw_document_already_deleted(
                        entity_or_document_id,
                        source_name,
                        "copy",
                        destination_entity_or_document_id,
                        destination_entity_or_document_id,
                    )

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        entity_or_document_id, CommandType.ATTACHMENT_DELETE, source_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "delete")

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        entity_or_document_id, CommandType.ATTACHMENT_MOVE, source_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "rename")

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_DELETE, destination_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "delete")

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_MOVE, destination_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "rename")

                self.__session.defer(
                    CopyAttachmentCommandData(
                        entity_or_document_id, source_name, destination_entity_or_document_id, destination_name, None
                    )
                )

            def rename(self, entity_or_document_id: Union[str, object], name: str, new_name: str) -> None:
                self.move(entity_or_document_id, name, entity_or_document_id, new_name)

            def move(
                self,
                source_entity_or_document_id: Union[str, object],
                source_name: str,
                destination_entity_or_document_id: Union[str, object],
                destination_name: str,
            ) -> None:
                if not isinstance(source_entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(source_entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(source_entity_or_document_id)
                    source_entity_or_document_id = entity.key

                if not isinstance(destination_entity_or_document_id, str):
                    entity = self.__session.documents_by_entity.get(destination_entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(destination_entity_or_document_id)
                    destination_entity_or_document_id = entity.key

                if source_entity_or_document_id is None or source_entity_or_document_id.isspace():
                    raise ValueError("Source entity id is required")
                if source_name is None or source_name.isspace():
                    raise ValueError("Source name is required")
                if destination_entity_or_document_id is None or destination_entity_or_document_id.isspace():
                    raise ValueError("Destination entity id is required")
                if destination_name is None or destination_name.isspace():
                    raise ValueError("Destination name is required")

                if (
                    source_entity_or_document_id.lower() == destination_entity_or_document_id.lower()
                    and source_name == destination_name
                ):
                    return  # no-op

                source_document = self.__session.documents_by_id.get_value(source_entity_or_document_id)
                if source_document is not None and source_document.entity in self.__session.deleted_entities:
                    self.__throw_document_already_deleted(
                        source_entity_or_document_id,
                        source_name,
                        "move",
                        destination_entity_or_document_id,
                        source_entity_or_document_id,
                    )

                destination_document = self.__session.documents_by_id.get_value(destination_entity_or_document_id)
                if destination_document is not None and destination_document.entity in self.__session.deleted_entities:
                    self.__throw_document_already_deleted(
                        source_entity_or_document_id,
                        source_name,
                        "move",
                        destination_entity_or_document_id,
                        destination_entity_or_document_id,
                    )

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        source_entity_or_document_id, CommandType.ATTACHMENT_DELETE, source_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, source_name, "rename", "delete"
                    )

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        source_entity_or_document_id, CommandType.ATTACHMENT_MOVE, source_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, source_name, "rename", "rename"
                    )

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_DELETE, destination_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, destination_name, "rename", "delete"
                    )

                if (
                    pyravendb.documents.IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_MOVE, destination_name
                    )
                    in self.__session.deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, destination_name, "rename", "rename"
                    )

                self.__session.defer(
                    MoveAttachmentCommandData(
                        source_entity_or_document_id,
                        source_name,
                        destination_entity_or_document_id,
                        destination_name,
                        None,
                    )
                )

            def get_names(self, entity: object) -> List[AttachmentName]:
                if not entity:
                    return []

                if isinstance(entity, str):
                    raise TypeError(
                        "get_names requires a tracked entity object, other types such as document id are not valid."
                    )

                document = self.__session.documents_by_entity.get(entity)
                if document is None:
                    self.throw_not_in_session(entity)

                attachments = document.metadata.get(constants.Documents.Metadata.ATTACHMENTS)
                if not attachments:
                    return []

                results = []

                for attachment in attachments:
                    results.append(AttachmentName.from_json(attachment))
                return results

            def __throw_document_already_deleted(
                self,
                document_id: str,
                name: str,
                operation: str,
                destination_document_id: str,
                deleted_document_id: str,
            ):
                raise InvalidOperationException(
                    f"Can't {operation} attachment '{name}' of document '{document_id}' " + ""
                    if not deleted_document_id
                    else f"to '{destination_document_id}'"
                    + f", the document '{deleted_document_id}' was already deleted in this session"
                )

            def __throw_other_deferred_command_exception(
                self, document_id: str, name: str, operation: str, previous_operation: str
            ):
                raise InvalidOperationException(
                    f"Can't {operation} attachment '{name}' of document '{document_id}', "
                    f"there is deferred command registered to {previous_operation} an attachment with '{name}' name."
                )

    class _DocumentQueryGenerator:
        def __init__(self, session: DocumentSession, conventions: DocumentConventions = None):
            self.__session = session
            self.__conventions = conventions

        @property
        def conventions(self) -> DocumentConventions:
            return self.__conventions

        @property
        def session(self) -> InMemoryDocumentSessionOperations:
            return self.__session

        def document_query(
            self,
            object_type: type,
            index_class_or_name: Union[type, str] = None,
            collection_name: str = None,
            is_map_reduce: bool = False,
            **kwargs,
        ):
            if isinstance(index_class_or_name, type):
                if not issubclass(index_class_or_name, AbstractCommonApiForIndexes):
                    raise TypeError(
                        f"Incorrect type, {index_class_or_name} isn't an index. It doesn't inherit from"
                        f" AbstractCommonApiForIndexes"
                    )
                # todo: check the code below
                index_class_or_name: AbstractCommonApiForIndexes = Utils.initialize_object(
                    None, index_class_or_name, True
                )
                index_class_or_name = index_class_or_name.index_name
            index_name_and_collection = self.session._process_query_parameters(
                object_type, index_class_or_name, collection_name, self.session.conventions
            )
            index_name = index_name_and_collection[0]
            collection_name = index_name_and_collection[1]

            q = Query(self.session)
            q(object_type, index_name, collection_name, is_map_reduce, **kwargs)
            return q
