from __future__ import annotations

import abc
import copy
from datetime import datetime
import http
import json
import os
import time
import uuid
from typing import (
    Union,
    Callable,
    TYPE_CHECKING,
    Optional,
    Dict,
    List,
    Type,
    TypeVar,
    Iterator,
    Iterable,
    Tuple,
    Generic,
    Set,
)

from ravendb.documents.session.document_session_revisions import DocumentSessionRevisions
from ravendb.primitives import constants
from ravendb.primitives.constants import int_max
from ravendb.documents.operations.counters import CounterOperation, CounterOperationType, GetCountersOperation
from ravendb.documents.operations.time_series import (
    TimeSeriesOperation,
    GetTimeSeriesOperation,
    TimeSeriesRangeResult,
    TimeSeriesDetails,
    GetMultipleTimeSeriesOperation,
    TimeSeriesConfiguration,
)
from ravendb.documents.commands.stream import StreamResult
from ravendb.documents.session.conditional_load import ConditionalLoadResult
from ravendb.documents.session.operations.stream import StreamOperation
from ravendb.documents.session.stream_statistics import StreamQueryStatistics
from ravendb.documents.session.tokens.query_tokens.definitions import FieldsToFetchToken
from ravendb.documents.session.time_series import (
    TimeSeriesEntry,
    TypedTimeSeriesEntry,
    TimeSeriesValuesHelper,
    TypedTimeSeriesRollupEntry,
    ITimeSeriesValuesBindable,
    TimeSeriesRange,
)
from ravendb.documents.time_series import TimeSeriesOperations
from ravendb.exceptions import exceptions
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.data.operation import AttachmentType
from ravendb.documents.indexes.definitions import AbstractCommonApiForIndexes
from ravendb.documents.operations.attachments import (
    GetAttachmentOperation,
    AttachmentName,
    CloseableAttachmentResult,
)
from ravendb.documents.operations.batch import BatchOperation
from ravendb.documents.operations.executor import OperationExecutor, SessionOperationExecutor
from ravendb.documents.operations.patch import PatchRequest
from ravendb.documents.queries.misc import Query
from ravendb.documents.session.cluster_transaction_operation import (
    ClusterTransactionOperations,
    IClusterTransactionOperations,
)
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.loaders.include import TimeSeriesIncludeBuilder
from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
    InMemoryDocumentSessionOperations,
)
from ravendb.documents.session.loaders.include import IncludeBuilder
from ravendb.documents.session.loaders.loaders import LoaderWithInclude, MultiLoaderWithInclude
from ravendb.documents.session.operations.lazy import LazyLoadOperation, LazySessionOperations
from ravendb.documents.session.operations.operations import MultiGetOperation, LoadStartingWithOperation
from ravendb.documents.session.operations.query import QueryOperation
from ravendb.documents.session.misc import (
    SessionOptions,
    ResponseTimeInformation,
    JavaScriptArray,
    JavaScriptMap,
    DocumentsChanges,
    SessionInfo,
    TransactionMode,
)
from ravendb.documents.session.query import DocumentQuery, RawDocumentQuery, AbstractDocumentQuery
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.documents.session.operations.load_operation import LoadOperation
from ravendb.tools.time_series import TSRangeHelper
from ravendb.tools.utils import Utils, Stopwatch, CaseInsensitiveDict
from ravendb.documents.commands.batches import (
    PatchCommandData,
    CommandType,
    DeleteCommandData,
    PutAttachmentCommandData,
    DeleteAttachmentCommandData,
    CopyAttachmentCommandData,
    MoveAttachmentCommandData,
    CommandData,
    IndexBatchOptions,
    ReplicationBatchOptions,
    CountersBatchCommandData,
    TimeSeriesBatchCommandData,
)
from ravendb.documents.commands.crud import (
    HeadDocumentCommand,
    GetDocumentsCommand,
    HeadAttachmentCommand,
    ConditionalGetDocumentsCommand,
)
from ravendb.documents.commands.multi_get import GetRequest

from ravendb.documents.store.lazy import Lazy
from ravendb.documents.store.misc import IdTypeAndName

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.operations.lazy.definition import LazyOperation
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.http.server_node import ServerNode

_T = TypeVar("_T")
_TIndex = TypeVar("_TIndex", bound=AbstractCommonApiForIndexes)
_T_TS_Values_Bindable = TypeVar("_T_TS_Values_Bindable", bound=ITimeSeriesValuesBindable)


class DocumentSession(InMemoryDocumentSessionOperations):
    def __init__(self, store: DocumentStore, key: uuid.UUID, options: SessionOptions):
        super().__init__(store, key, options)

        self._advanced: DocumentSession._Advanced = DocumentSession._Advanced(self)
        self._document_query_generator: DocumentSession._DocumentQueryGenerator = (
            DocumentSession._DocumentQueryGenerator(self)
        )
        self._cluster_transaction: Optional[ClusterTransactionOperations] = None
        self._revisions: Optional[DocumentSessionRevisions] = None

    @property
    def advanced(self):
        return self._advanced

    @property
    def _lazily(self):
        return LazySessionOperations(self)

    @property
    def cluster_transaction(self) -> ClusterTransactionOperations:
        if self._cluster_transaction is None:
            self._cluster_transaction = ClusterTransactionOperations(self)
        return self._cluster_transaction

    @property
    def has_cluster_session(self) -> bool:
        return self._cluster_transaction is not None

    @property
    def operations(self) -> OperationExecutor:
        if self._operation_executor is None:
            self._operation_executor = SessionOperationExecutor(self)
        return self._operation_executor

    def save_changes(self) -> None:
        save_changes_operation = BatchOperation(self)
        command = save_changes_operation.create_request()
        if command:
            with command:
                if command is None:
                    return

                if self.no_tracking:
                    raise RuntimeError("Cannot execute save_changes when entity tracking is disabled.")

                self._request_executor.execute_command(command, self.session_info)
                self.update_session_after_save_changes(command.result)
                save_changes_operation.set_result(command.result)

    def _has_cluster_session(self) -> bool:
        return self._cluster_transaction is not None

    def _clear_cluster_session(self) -> None:
        if not self._has_cluster_session():
            return
        self.cluster_transaction.clear()

    def _generate_id(self, entity: object) -> str:
        return self.conventions.generate_document_id(self.database_name, entity)

    def execute_all_pending_lazy_operations(self) -> ResponseTimeInformation:
        requests = []
        for i in range(len(self._pending_lazy_operations)):
            # todo: pending lazy operation create request - WIP
            req = self._pending_lazy_operations[i].create_request()
            if req is None:
                self._pending_lazy_operations.pop(i)
                i -= 1
                continue
            requests.append(req)
        if not requests:
            return ResponseTimeInformation()

        sw = Stopwatch.create_started()
        response_time_duration = ResponseTimeInformation()
        while self._execute_lazy_operations_single_step(response_time_duration, requests, sw):
            time.sleep(0.1)
        response_time_duration.compute_server_total()

        for pending_lazy_operation in self._pending_lazy_operations:
            value = self._on_evaluate_lazy.get(pending_lazy_operation)
            if value is not None:
                value(pending_lazy_operation.result)

        elapsed = sw.elapsed()
        response_time_duration.total_client_duration = elapsed

        self._pending_lazy_operations.clear()

        return response_time_duration

    def _execute_lazy_operations_single_step(
        self, response_time_information: ResponseTimeInformation, get_requests: List[GetRequest], sw: Stopwatch
    ) -> bool:
        multi_get_operation = MultiGetOperation(self)
        with multi_get_operation.create_request(get_requests) as multi_get_command:
            self._request_executor.execute_command(multi_get_command, self.session_info)

            responses = multi_get_command.result

            if not multi_get_command.aggressively_cached:
                self.increment_requests_count()

            for i in range(len(self._pending_lazy_operations)):
                response = responses[i]

                temp_req_time = response.headers.get(constants.Headers.REQUEST_TIME)
                response.elapsed = sw.elapsed()
                total_time = temp_req_time if temp_req_time is not None else 0

                time_item = ResponseTimeInformation.ResponseTimeItem(get_requests[i].url_and_query, total_time)

                response_time_information.duration_breakdown.append(time_item)

                if response.request_has_errors:
                    raise RuntimeError(
                        f"Got an error from server, status code: {response.status_code}{os.linesep}{response.result}"
                    )

                self._pending_lazy_operations[i].handle_response(response)
                if self._pending_lazy_operations[i].requires_retry:
                    return True

            return False

    def include(self, path: str) -> LoaderWithInclude:
        return MultiLoaderWithInclude(self).include(path)

    def add_lazy_operation(
        self, object_type: Type[_T], operation: LazyOperation, on_eval: Optional[Callable[[_T], None]]
    ) -> Lazy[_T]:
        self._pending_lazy_operations.append(operation)

        def __supplier():
            self.advanced.eagerly.execute_all_pending_lazy_operations()
            return self._get_operation_result(object_type, operation.result)

        lazy_value = Lazy(__supplier)

        if on_eval is not None:
            self._on_evaluate_lazy[operation] = lambda the_result: on_eval(
                self._get_operation_result(object_type, the_result)
            )

        return lazy_value

    def add_lazy_count_operation(self, operation: LazyOperation[_T]) -> Lazy[_T]:
        self._pending_lazy_operations.append(operation)

        def __supplier():
            self.advanced.eagerly.execute_all_pending_lazy_operations()
            return operation.query_result.total_results

        return Lazy(__supplier)

    def lazy_load_internal(
        self,
        object_type: Type[_T],
        keys: List[str],
        includes: List[str],
        on_eval: Optional[Callable[[Dict[str, _T]], None]],
    ) -> Lazy[_T]:
        if self.check_if_id_already_included(keys, includes):
            return Lazy(lambda: self.load(keys, object_type))

        load_operation = LoadOperation(self).by_keys(keys).with_includes(includes)
        lazy_op = LazyLoadOperation(object_type, self, load_operation).by_keys(keys).with_includes(*includes)
        return self.add_lazy_operation(dict, lazy_op, on_eval)

    def load(
        self,
        key_or_keys: Union[List[str], str],
        object_type: Optional[Type[_T]] = None,
        includes: Callable[[IncludeBuilder], None] = None,
    ) -> Union[Dict[str, _T], _T]:
        if key_or_keys is None:
            return None  # todo: return default value of object_type, not always None

        if includes is None:
            load_operation = LoadOperation(self)
            self._load_internal_stream(
                [key_or_keys] if isinstance(key_or_keys, str) else key_or_keys, load_operation, None
            )
            result = load_operation.get_documents(object_type)
            return result.popitem()[1] if len(result) == 1 else result if result else None

        include_builder = IncludeBuilder(self.conventions)
        includes(include_builder)

        time_series_includes = (
            include_builder.time_series_to_include if include_builder.time_series_to_include is not None else None
        )
        compare_exchange_values_to_include = include_builder.compare_exchange_values_to_include

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
        object_type: Type[_T],
        keys: List[str],
        includes: Optional[Set[str]],
        counter_includes: List[str] = None,
        include_all_counters: bool = False,
        time_series_includes: List[TimeSeriesRange] = None,
        compare_exchange_value_includes: List[str] = None,
    ) -> Dict[str, _T]:
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
            self._request_executor.execute_command(command, self.session_info)
            load_operation.set_result(command.result)

        return load_operation.get_documents(object_type)

    def _load_internal_stream(self, keys: List[str], operation: LoadOperation, stream: Optional[bytes] = None) -> None:
        operation.by_keys(keys)

        command = operation.create_request()

        if command:
            self._request_executor.execute_command(command, self.session_info)

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
        id_prefix: str,
        object_type: Optional[Type[_T]] = None,
        matches: Optional[str] = None,
        start: Optional[int] = None,
        page_size: Optional[int] = None,
        exclude: Optional[str] = None,
        start_after: Optional[str] = None,
    ) -> List[_T]:
        load_starting_with_operation = LoadStartingWithOperation(self)
        self._load_starting_with_internal(
            id_prefix, load_starting_with_operation, matches, start, page_size, exclude, start_after
        )
        return load_starting_with_operation.get_documents(object_type)

    def load_starting_with_into_stream(
        self,
        id_prefix: str,
        matches: str = None,
        start: int = 0,
        page_size: int = 25,
        exclude: str = None,
        start_after: str = None,
    ) -> bytes:
        if id_prefix is None:
            raise ValueError("Arg 'id_prefix' is cannot be None.")
        return self._load_starting_with_into_stream_internal(
            id_prefix, LoadStartingWithOperation(self), matches, start, page_size, exclude, start_after
        )

    def _load_starting_with_internal(
        self,
        id_prefix: str,
        operation: LoadStartingWithOperation,
        matches: str,
        start: int,
        page_size: int,
        exclude: str,
        start_after: str,
    ) -> GetDocumentsCommand:
        operation.with_start_with(id_prefix, matches, start, page_size, exclude, start_after)
        command = operation.create_request()
        if command:
            self._request_executor.execute_command(command, self.session_info)
            operation.set_result(command.result)
        return command

    def _load_starting_with_into_stream_internal(
        self,
        id_prefix: str,
        operation: LoadStartingWithOperation,
        matches: str,
        start: int,
        page_size: int,
        exclude: str,
        start_after: str,
    ) -> bytes:
        operation.with_start_with(id_prefix, matches, start, page_size, exclude, start_after)
        command = operation.create_request()
        bytes_result = None
        if command:
            self.request_executor.execute_command(command, self.session_info)
            try:
                result = command.result
                bytes_result = json.dumps(result.to_json()).encode("utf-8")
            except Exception as e:
                raise RuntimeError("Unable sto serialize returned value into stream") from e
        return bytes_result

    def document_query_from_index_type(self, index_type: Type[_TIndex], object_type: Type[_T]) -> DocumentQuery[_T]:
        try:
            index = Utils.try_get_new_instance(index_type)
            return self.document_query(index.index_name, None, object_type, index.is_map_reduce)
        except Exception as e:
            raise RuntimeError(f"unable to query index: {index_type.__name__} {e.args[0]}", e)

    def document_query(
        self,
        index_name: Optional[str] = None,
        collection_name: Optional[str] = None,
        object_type: Optional[Type[_T]] = None,
        is_map_reduce: bool = False,
    ) -> DocumentQuery[_T]:
        index_name_and_collection = self._process_query_parameters(
            object_type, index_name, collection_name, self.conventions
        )
        index_name = index_name_and_collection[0]
        collection_name = index_name_and_collection[1]

        return DocumentQuery(object_type, self, index_name, collection_name, is_map_reduce)

    def query(self, source: Optional[Query] = None, object_type: Optional[Type[_T]] = None) -> DocumentQuery[_T]:
        if source is not None:
            if source.collection and not source.collection.isspace():
                return self.document_query(None, source.collection, object_type, False)

            if source.index_name and not source.index_name.isspace():
                return self.document_query(source.index_name, None, object_type, False)

            return self.document_query_from_index_type(source.index_type, object_type)

        return self.document_query(None, None, object_type, False)

    def query_collection(self, collection_name: str, object_type: Optional[Type[_T]] = None) -> DocumentQuery[_T]:
        if not collection_name.isspace():
            return self.query(Query.from_collection_name(collection_name), object_type)
        raise ValueError("collection_name cannot be None or whitespace")

    def query_index(self, index_name: str, object_type: Optional[Type[_T]] = None) -> DocumentQuery[_T]:
        if not index_name.isspace():
            return self.query(Query.from_index_name(index_name), object_type)
        raise ValueError("index_name cannot be None or whitespace")

    def query_index_type(self, index_type: Type[_TIndex], object_type: Optional[Type[_T]] = None) -> DocumentQuery[_T]:
        if index_type is None or not isinstance(index_type, AbstractCommonApiForIndexes):
            return self.query(Query.from_index_type(index_type), object_type)

    def counters_for(self, document_id: str) -> SessionDocumentCounters:
        return SessionDocumentCounters(self, document_id)

    def counters_for_entity(self, entity: object) -> SessionDocumentCounters:
        return SessionDocumentCounters(self, entity)

    def time_series_for(self, document_id: str, name: str = None) -> SessionDocumentTimeSeries:
        if not isinstance(document_id, str):
            raise TypeError("Method time_series_for expects a string. Did you want to call time_series_for_entity?")
        return SessionDocumentTimeSeries(self, document_id, name)

    def time_series_for_entity(self, entity: object, name: str = None) -> SessionDocumentTimeSeries:
        return SessionDocumentTimeSeries.from_entity(self, entity, name)

    def typed_time_series_for(
        self, ts_bindable_object_type: Type[_T_TS_Values_Bindable], document_id: str, name: Optional[str] = None
    ) -> SessionDocumentTypedTimeSeries[_T_TS_Values_Bindable]:
        ts_name = name or TimeSeriesOperations.get_time_series_name(ts_bindable_object_type, self.conventions)
        return SessionDocumentTypedTimeSeries(ts_bindable_object_type, self, document_id, ts_name)

    def typed_time_series_for_entity(
        self, ts_bindable_object_type: Type[_T_TS_Values_Bindable], entity: object, name: Optional[str] = None
    ) -> SessionDocumentTypedTimeSeries[_T_TS_Values_Bindable]:
        ts_name = name or TimeSeriesOperations.get_time_series_name(ts_bindable_object_type, self.conventions)
        return SessionDocumentTypedTimeSeries.from_entity_typed(ts_bindable_object_type, self, entity, ts_name)

    def time_series_rollup_for(
        self,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        document_id: str,
        policy: str,
        raw: Optional[str] = None,
    ) -> SessionDocumentRollupTypedTimeSeries[_T_TS_Values_Bindable]:
        ts_name = raw or TimeSeriesOperations.get_time_series_name(ts_bindable_object_type, self.conventions)
        return SessionDocumentRollupTypedTimeSeries(
            ts_bindable_object_type,
            self,
            document_id,
            ts_name + TimeSeriesConfiguration.TIME_SERIES_ROLLUP_SEPARATOR + policy,
        )

    def time_series_rollup_for_entity(
        self,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        entity: object,
        policy: str,
        raw: Optional[str] = None,
    ) -> SessionDocumentRollupTypedTimeSeries[_T_TS_Values_Bindable]:
        ts_name = raw or TimeSeriesOperations.get_time_series_name(ts_bindable_object_type, self.conventions)
        return SessionDocumentRollupTypedTimeSeries.from_entity_rollup_typed(
            ts_bindable_object_type,
            self,
            entity,
            ts_name + TimeSeriesConfiguration.TIME_SERIES_ROLLUP_SEPARATOR + policy,
        )

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

            sw = Stopwatch.create_started()
            response_time_duration = ResponseTimeInformation()
            while self.__session._execute_lazy_operations_single_step(response_time_duration, requests, sw):
                time.sleep(0.1)
            response_time_duration.compute_server_total()

            for pending_lazy_operation in self.__session._pending_lazy_operations:
                value = self.__session._on_evaluate_lazy.get(pending_lazy_operation)
                if value is not None:
                    value(pending_lazy_operation.result)

            elapsed = datetime.now() - sw.elapsed()
            response_time_duration.total_client_duration = elapsed

            self.__session._pending_lazy_operations.clear()

            return response_time_duration

    class _Advanced:
        def __init__(self, session: DocumentSession):
            self._session = session
            self.__values_count = 0
            self.__custom_count = 0
            self.__attachment = None

            self.eagerly = DocumentSession._EagerSessionOperations(session)

        @property
        def request_executor(self) -> RequestExecutor:
            return self._session._request_executor

        @property
        def document_store(self) -> DocumentStore:
            return self._session._document_store

        @property
        def session_info(self) -> SessionInfo:
            return self._session.session_info

        @property
        def attachments(self):
            if not self.__attachment:
                self.__attachment = self._Attachment(self._session)
            return self.__attachment

        @property
        def revisions(self):
            if self._session._revisions is None:
                self._session._revisions = DocumentSessionRevisions(self._session)
            return self._session._revisions

        @property
        def cluster_transaction(self) -> IClusterTransactionOperations:
            return self._session.cluster_transaction

        @property
        def number_of_requests(self) -> int:
            return self._session.number_of_requests

        @property
        def store_identifier(self) -> str:
            return self._session._store_identifier

        @property
        def transaction_mode(self) -> TransactionMode:
            return self._session.transaction_mode

        @transaction_mode.setter
        def transaction_mode(self, value: TransactionMode):
            self._session.transaction_mode = value

        @property
        def use_optimistic_concurrency(self) -> bool:
            return self._session._use_optimistic_concurrency

        @use_optimistic_concurrency.setter
        def use_optimistic_concurrency(self, value: bool):
            self._session._use_optimistic_concurrency = value

        def is_loaded(self, key: str) -> bool:
            return self._session.is_loaded_or_deleted(key)

        def has_changed(self, entity: object) -> bool:
            document_info = self._session._documents_by_entity.get(entity)

            if document_info is None:
                return False

            document = self._session.entity_to_json.convert_entity_to_json(entity, document_info)
            return self._session._entity_changed(document, document_info, None)

        def get_metadata_for(self, entity: object) -> MetadataAsDictionary:
            if entity is None:
                raise ValueError("Entity cannot be None")
            document_info = self._session._get_document_info(entity)
            if document_info.metadata_instance:
                return document_info.metadata_instance
            metadata_as_json = document_info.metadata
            metadata = MetadataAsDictionary(metadata=metadata_as_json)
            document_info.metadata_instance = metadata
            return metadata

        def get_counters_for(self, entity: object) -> List[str]:
            if entity is None:
                raise ValueError("Entity cannot be None")
            document_info = self._session._get_document_info(entity)

            counters_array = document_info.metadata.get(constants.Documents.Metadata.COUNTERS)
            return counters_array if counters_array else None

        def get_time_series_for(self, entity: object) -> List[str]:
            if not entity:
                raise ValueError("Instance cannot be None")
            document_info = self._session._get_document_info(entity)
            array = document_info.metadata.get(constants.Documents.Metadata.TIME_SERIES)
            return list(map(str, array)) if array else []

        def get_change_vector_for(self, entity: object) -> str:
            if not entity:
                raise ValueError("Entity cannot be None")
            return self._session._get_document_info(entity).metadata.get(constants.Documents.Metadata.CHANGE_VECTOR)

        def get_last_modified_for(self, entity: object) -> datetime:
            if entity is None:
                raise ValueError("Entity cannot be None")
            document_info = self._session._get_document_info(entity)
            last_modified = document_info.metadata.get(constants.Documents.Metadata.LAST_MODIFIED)
            if last_modified is not None and last_modified is not None:
                return Utils.string_to_datetime(last_modified)

        def get_document_id(self, entity: object) -> Union[None, str]:
            if entity is None:
                return None

            value = self._session._documents_by_entity.get(entity)
            return value.key if value is not None else None

        def evict(self, entity: object) -> None:
            document_info = self._session._documents_by_entity.get(entity)
            if document_info is not None:
                self._session._documents_by_entity.evict(entity)
                self._session._documents_by_id.pop(document_info.key, None)

                if self._session._counters_by_doc_id:
                    self._session._counters_by_doc_id.pop(document_info.key, None)
                if self._session.time_series_by_doc_id:
                    self._session.time_series_by_doc_id.pop(document_info.key, None)

            self._session._deleted_entities.evict(entity)
            self._session.entity_to_json.remove_from_missing(entity)

        def defer(self, *commands: CommandData) -> None:
            self._session.defer(*commands)

        def clear(self) -> None:
            self._session._documents_by_entity.clear()
            self._session._deleted_entities.clear()
            self._session._documents_by_id.clear()
            self._session._known_missing_ids.clear()
            if self._session._counters_by_doc_id:
                self._session._counters_by_doc_id.clear()
            self._session._deferred_commands.clear()
            self._session._deferred_commands_map.clear()
            self._session._clear_cluster_session()
            self._session._pending_lazy_operations.clear()
            self._session.entity_to_json.clear()

        def document_query(
            self,
            index_name: str = None,
            collection_name: str = None,
            object_type: Type[_T] = None,
            is_map_reduce: bool = False,
        ) -> DocumentQuery[_T]:
            return self._session.document_query(index_name, collection_name, object_type, is_map_reduce)

        def document_query_from_index_type(self, index_type: Type[_TIndex], object_type: Type[_T]) -> DocumentQuery[_T]:
            return self._session.document_query_from_index_type(index_type, object_type)

        def ignore_changes_for(self, entity: object) -> None:
            self._session.ignore_changes_for(entity)

        def has_changes(self) -> bool:
            return self._session.has_changes()

        def refresh(self, entity: object) -> object:
            document_info = self._session._documents_by_entity.get(entity)
            if document_info is None:
                raise ValueError("Cannot refresh a transient instance")
            self._session.increment_requests_count()

            command = GetDocumentsCommand.from_single_id(document_info.key, None, False)
            self._session._request_executor.execute_command(command, self._session.session_info)
            entity = self._session._refresh_internal(entity, command, document_info)
            return entity

        def raw_query(self, query: str, object_type: Optional[Type[_T]] = None) -> RawDocumentQuery[_T]:
            return RawDocumentQuery(object_type, self._session, query)

        def get_current_session_node(self) -> ServerNode:
            return self.session_info.get_current_session_node(self.request_executor)

        @property
        def lazily(self) -> LazySessionOperations:
            return self._session._lazily

        def graph_query(self, object_type: type, query: str):  # -> GraphDocumentQuery:
            raise NotImplementedError("Dropped support for graph queries")

        def what_changed(self) -> Dict[str, List[DocumentsChanges]]:
            return self._session._what_changed()

        def exists(self, key: str) -> bool:
            if key is None:
                raise ValueError("Key cannot be None")
            if key in self._session._known_missing_ids:
                return False

            if self._session._documents_by_id.get(key) is not None:
                return True

            command = HeadDocumentCommand(key, None)
            self._session._request_executor.execute_command(command, self._session.session_info)
            return command.result is not None

        def wait_for_replication_after_save_changes(
            self,
            options: Callable[
                [InMemoryDocumentSessionOperations.ReplicationWaitOptsBuilder], None
            ] = lambda options: None,
        ) -> None:
            builder = self._session.ReplicationWaitOptsBuilder(self._session)
            options(builder)

            builder_options = builder.get_options()
            replication_options = builder_options.replication_options
            if replication_options is None:
                builder_options.replication_options = ReplicationBatchOptions()

            if replication_options.wait_for_replicas_timeout is None:
                replication_options.wait_for_replicas_timeout = (
                    self._session.conventions.wait_for_replication_after_save_changes_timeout
                )
            replication_options.wait_for_replicas = True

        def wait_for_indexes_after_save_changes(
            self,
            options: Callable[[InMemoryDocumentSessionOperations.IndexesWaitOptsBuilder], None] = lambda options: None,
        ):
            builder = InMemoryDocumentSessionOperations.IndexesWaitOptsBuilder(self._session)
            options(builder)

            builder_options = builder.get_options()
            index_options = builder_options.index_options

            if index_options is None:
                builder_options.index_options = IndexBatchOptions()

            if index_options.wait_for_indexes_timeout is None:
                index_options.wait_for_indexes_timeout = (
                    self._session.conventions.wait_for_indexes_after_save_changes_timeout
                )

            index_options.wait_for_indexes = True

        def load_starting_with(
            self,
            id_prefix: str,
            object_type: Optional[Type[_T]] = None,
            matches: Optional[str] = None,
            start: Optional[int] = None,
            page_size: Optional[int] = None,
            exclude: Optional[str] = None,
            start_after: Optional[str] = None,
        ) -> List[_T]:
            return self._session.load_starting_with(
                id_prefix, object_type, matches, start, page_size, exclude, start_after
            )

        def load_starting_with_into_stream(
            self,
            id_prefix: str,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            exclude: str = None,
            start_after: str = None,
        ) -> bytes:
            return self._session.load_starting_with_into_stream(
                id_prefix, matches, start, page_size, exclude, start_after
            )

        def load_into_stream(self, keys: List[str], output: bytes) -> None:
            if keys is None:
                raise ValueError("Keys cannot be None")

            self._session._load_internal_stream(keys, LoadOperation(self._session), output)

        def __try_merge_patches(self, document_key: str, patch_request: PatchRequest) -> bool:
            command = self._session._deferred_commands_map.get(
                IdTypeAndName.create(document_key, CommandType.PATCH, None)
            )
            if command is None:
                return False

            self._session._deferred_commands.remove(command)
            # We'll overwrite the deferredCommandsMap when calling Defer
            # No need to call deferredCommandsMap.remove((id, CommandType.PATCH, null));
            if not isinstance(command, PatchCommandData):
                raise TypeError(f"Command class {command.__class__.name} is invalid")

            old_patch: PatchCommandData = command
            new_script = old_patch.patch.script + "\n" + patch_request.script
            new_values = dict(old_patch.patch.values)

            for key, value in patch_request.values.items():
                new_values[key] = value

            new_patch_request = PatchRequest()
            new_patch_request.script = new_script
            new_patch_request.values = new_values

            self.defer(PatchCommandData(document_key, None, new_patch_request, None))
            return True

        def increment(self, key_or_entity: Union[object, str], path: str, value_to_add: object) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))
            patch_request = PatchRequest()
            variable = f"this.{path}"
            value = f"args.val_{self.__values_count}"
            patch_request.script = f"{variable} = {variable} ? {variable} + {value} : {value} ;"
            patch_request.values = {f"val_{self.__values_count}": value_to_add}
            self.__values_count += 1
            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch(self, key_or_entity: Union[object, str], path: str, value: object) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.get_metadata_for(key_or_entity)
                key_or_entity = metadata[constants.Documents.Metadata.ID]

            patch_request = PatchRequest()
            patch_request.script = f"this.{path} = args.val_{self.__values_count};"
            patch_request.values = {f"val_{self.__values_count}": value}

            self.__values_count += 1

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch_array(
            self, key_or_entity: Union[object, str], path_to_array: str, array_adder: Callable[[JavaScriptArray], None]
        ) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))

            script_array = JavaScriptArray(self.__custom_count, path_to_array)
            self.__custom_count += 1

            array_adder(script_array)

            patch_request = PatchRequest()
            patch_request.script = script_array.script
            patch_request.values = script_array.parameters

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def patch_object(
            self,
            key_or_entity: Union[object, str],
            path_to_object: str,
            dictionary_adder: Callable[[JavaScriptMap], None],
        ) -> None:
            if not isinstance(key_or_entity, str):
                metadata = self.get_metadata_for(key_or_entity)
                key_or_entity = str(metadata.get(constants.Documents.Metadata.ID))

            script_map = JavaScriptMap(self.__custom_count, path_to_object)
            self.__custom_count += 1

            dictionary_adder(script_map)

            patch_request = PatchRequest()
            patch_request.script = script_map.script
            patch_request.values = script_map.parameters

            if not self.__try_merge_patches(key_or_entity, patch_request):
                self.defer(PatchCommandData(key_or_entity, None, patch_request, None))

        def add_or_patch(self, key: str, entity: object, path_to_object: str, value: object) -> None:
            patch_request = PatchRequest()
            patch_request.script = f"this.{path_to_object} = args.val_{self.__values_count}"
            patch_request.values = {f"val_{self.__values_count}": value}

            collection_name = self._session._request_executor.conventions.get_collection_name(entity)
            python_type = self._session._request_executor.conventions.get_python_class_name(type(entity))

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self._session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__values_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.defer(patch_command_data)

        def add_or_patch_array(
            self, key: str, entity: object, path_to_array: str, list_adder: Callable[[JavaScriptArray], None]
        ) -> None:
            script_array = JavaScriptArray(self.__custom_count, path_to_array)
            self.__custom_count += 1

            list_adder(script_array)

            patch_request = PatchRequest()
            patch_request.script = script_array.script
            patch_request.values = script_array.parameters

            collection_name = self._session._request_executor.conventions.get_collection_name(entity)
            python_type = self._session._request_executor.conventions.get_python_class_name(type(entity))

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self._session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__values_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.defer(patch_command_data)

        def add_or_increment(self, key: str, entity: object, path_to_object: str, val_to_add: object) -> None:
            variable = f"this.{path_to_object}"
            value = f"args.val_{self.__values_count}"

            patch_request = PatchRequest()
            patch_request.script = f"{variable} = {variable} ? {variable} + {value} : {value}"
            patch_request.values = {f"val_{self.__values_count}": val_to_add}

            collection_name = self._session._request_executor.conventions.get_collection_name(entity)
            python_type = self._session._request_executor.conventions.find_python_class_name(entity.__class__)

            metadata_as_dictionary = MetadataAsDictionary()
            metadata_as_dictionary[constants.Documents.Metadata.COLLECTION] = collection_name
            metadata_as_dictionary[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type

            document_info = DocumentInfo(key, collection=collection_name, metadata_instance=metadata_as_dictionary)

            new_instance = self._session.entity_to_json.convert_entity_to_json(entity, document_info)

            self.__values_count += 1

            patch_command_data = PatchCommandData(key, None, patch_request)
            patch_command_data.create_if_missing = new_instance
            self.defer(patch_command_data)

        def stream(self, query_or_raw_query: AbstractDocumentQuery[_T]) -> Iterator[StreamResult[_T]]:
            stream_operation = StreamOperation(self._session)
            command = stream_operation.create_request(query_or_raw_query.index_query)

            self.request_executor.execute_command(command, self.session_info)

            result = stream_operation.set_result(command.result)

            return self._yield_result(query_or_raw_query, result)

        def stream_with_statistics(
            self, query_or_raw_query: AbstractDocumentQuery[_T], stats_callback: Callable[[StreamQueryStatistics], None]
        ) -> Iterator[StreamResult[_T]]:
            stats = StreamQueryStatistics()
            stream_operation = StreamOperation(self._session, stats)
            command = stream_operation.create_request(query_or_raw_query.index_query)

            self.request_executor.execute_command(command, self.session_info)

            result = stream_operation.set_result(command.result)

            stats_callback(stats)

            return self._yield_result(query_or_raw_query, result)

        def stream_starting_with(
            self,
            starts_with: str,
            matches: Optional[str] = None,
            start: int = 0,
            page_size: int = constants.int_max,
            start_after: Optional[str] = None,
            object_type: Optional[Type[_T]] = None,
        ) -> Iterable[StreamResult[_T]]:
            stream_operation = StreamOperation(self._session)

            command = stream_operation.create_request_for_starts_with(
                starts_with, matches, start, page_size, None, start_after
            )
            self.request_executor.execute_command(command, self.session_info)

            result = stream_operation.set_result(command.result)
            return DocumentSession._Advanced._StreamIterator(self, result, None, False, None, object_type)

        class _StreamIterator(Iterator):
            def __init__(
                self,
                session_advanced_reference: DocumentSession._Advanced,
                inner_iterator: Iterator[Dict],
                fields_to_fetch_token: Optional[FieldsToFetchToken],
                is_project_into: bool,
                on_next_item: Optional[Callable[[Dict], None]] = None,
                object_type: Optional[Type[_T]] = None,
            ):
                self._session_advanced_reference = session_advanced_reference
                self._inner_iterator = inner_iterator
                self._fields_to_fetch_token = fields_to_fetch_token
                self._is_project_into = is_project_into
                self._on_next_item = on_next_item
                self._object_type = object_type

            def __iter__(self):
                return self

            def __next__(self):
                next_value = self._inner_iterator.__next__()
                try:
                    if self._on_next_item is not None:
                        self._on_next_item(next_value)
                    return DocumentSession._Advanced._create_stream_result(
                        self._session_advanced_reference,
                        next_value,
                        self._fields_to_fetch_token,
                        self._is_project_into,
                        self._object_type,
                    )
                except Exception as e:
                    raise RuntimeError(f"Unable to parse stream result: {e.args[0]}", e)

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        def _create_stream_result(
            self,
            json_dict: Dict,
            fields_to_fetch: FieldsToFetchToken,
            is_project_into: bool,
            object_type: Optional[Type[_T]],
        ) -> StreamResult[_T]:
            metadata = json_dict.get(constants.Documents.Metadata.KEY)
            change_vector = metadata.get(constants.Documents.Metadata.CHANGE_VECTOR)

            # MapReduce indexes return reduce results tht don't have @id property
            key = metadata.get(constants.Documents.Metadata.ID, None)

            entity = QueryOperation.deserialize(
                object_type, key, json_dict, metadata, fields_to_fetch, True, self._session, is_project_into
            )

            stream_result = StreamResult(key, change_vector, MetadataAsDictionary(metadata), entity)
            return stream_result

        def _yield_result(self, query: AbstractDocumentQuery, enumerator: Iterator[Dict]) -> Iterator[StreamResult[_T]]:
            return DocumentSession._Advanced._StreamIterator(
                self,
                enumerator,
                query._fields_to_fetch_token,
                query.is_project_into,
                query.invoke_after_stream_executed,
                object_type=query.query_class,
            )

        def stream_into(self):  # query: Union[DocumentQuery, RawDocumentQuery], output: iter):
            pass

        def conditional_load(
            self, key: str, change_vector: str, object_type: Type[_T] = None
        ) -> ConditionalLoadResult[_T]:
            if key is None or key.isspace():
                raise ValueError("Key cannot be None")

            if self.is_loaded(key):
                entity = self._session.load(key, object_type)
                if entity is None:
                    return ConditionalLoadResult.create(None, None)

                cv = self.get_change_vector_for(entity)
                return ConditionalLoadResult.create(entity, cv)

            if change_vector is None or change_vector.isspace():
                raise ValueError(
                    f"The requested document with id '{key}' is not loaded into the session "
                    f"and could not conditional load when change_vector is None or empty."
                )

            self._session.increment_requests_count()

            cmd = ConditionalGetDocumentsCommand(key, change_vector)
            self._session._request_executor.execute_command(cmd)

            if cmd.status_code == http.HTTPStatus.NOT_MODIFIED:
                return ConditionalLoadResult.create(None, change_vector)  # value not changed

            elif cmd.status_code == http.HTTPStatus.NOT_FOUND:
                self._session.register_missing(key)
                return ConditionalLoadResult.create(None, None)  # value is missing

            document_info = DocumentInfo.get_new_document_info(cmd.result.results[0])
            r = self._session.track_entity_document_info(object_type, document_info)
            return ConditionalLoadResult.create(r, cmd.result.change_vector)

            # todo: stream, query and fors like time_series_rollup_for, conditional load

        class _Attachment:
            def __init__(self, session: DocumentSession):
                self.__session = session
                self.__store = session._document_store
                self.__defer_commands = session._deferred_commands

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
                self.__session._request_executor.execute_command(command, self.__session.session_info)
                return command.result is not None

            def store(
                self,
                entity_or_document_id: Union[object, str],
                name: str,
                stream: bytes,
                content_type: str = None,
                change_vector: str = None,
            ):
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session._documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.key

                if not entity_or_document_id:
                    raise ValueError(entity_or_document_id)
                if not name:
                    raise ValueError(name)

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.DELETE, None)
                ) in self.__session._deferred_commands_map:
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "legacy", "delete")

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_PUT, name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "legacy", "create")

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_DELETE, name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "legacy", "delete")

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_MOVE, name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "legacy", "rename")

                document_info = self.__session._documents_by_id.get_value(entity_or_document_id)
                if document_info and document_info.entity in self.__session._deleted_entities:
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
                    entity = self.__session._documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.metadata["@id"]

                if not entity_or_document_id:
                    raise ValueError(entity_or_document_id)
                if not name:
                    raise ValueError(name)

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.DELETE, None)
                    in self.__session._deferred_commands_map
                    or IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_DELETE, name)
                    in self.__session._deferred_commands_map
                ):
                    return

                document_info = self.__session._documents_by_id.get_value(entity_or_document_id)
                if document_info and document_info.entity in self.__session._deleted_entities:
                    return

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_PUT, name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "delete", "create")

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_MOVE, name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, name, "delete", "rename")

                self.__session.defer(DeleteAttachmentCommandData(entity_or_document_id, name, None))

            def get(
                self,
                entity_or_document_id: str = None,
                name: str = None,
                # att_requests: Optional[List[AttachmentRequest]] = None,
                # todo: fetching multiple attachments with single command
            ) -> CloseableAttachmentResult:
                # if att_requests is not None:
                #     if entity_or_document_id or name:
                #         raise ValueError("Specify either <att_requests> or <entity/document_id, name>")
                #     operation = GetAttachmentsOperation(att_requests, AttachmentType.document)
                # else:
                if not isinstance(entity_or_document_id, str):
                    entity = self.__session._documents_by_entity.get(entity_or_document_id, None)
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
                    entity = self.__session._documents_by_entity.get(entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(entity_or_document_id)
                    entity_or_document_id = entity.key

                if not isinstance(destination_entity_or_document_id, str):
                    entity = self.__session._documents_by_entity.get(destination_entity_or_document_id, None)
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

                source_document = self.__session._documents_by_id.get_value(entity_or_document_id)
                if entity_or_document_id and source_document.entity in self.__session._deleted_entities:
                    self.__throw_document_already_deleted(
                        entity_or_document_id,
                        source_name,
                        "copy",
                        destination_entity_or_document_id,
                        entity_or_document_id,
                    )

                destination_document = self.__session._documents_by_id.get_value(destination_entity_or_document_id)
                if destination_document and destination_document.entity in self.__session._deleted_entities:
                    self.__throw_document_already_deleted(
                        entity_or_document_id,
                        source_name,
                        "copy",
                        destination_entity_or_document_id,
                        destination_entity_or_document_id,
                    )

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_DELETE, source_name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "delete")

                if (
                    IdTypeAndName.create(entity_or_document_id, CommandType.ATTACHMENT_MOVE, source_name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "rename")

                if (
                    IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_DELETE, destination_name
                    )
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(entity_or_document_id, source_name, "copy", "delete")

                if (
                    IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_MOVE, destination_name
                    )
                    in self.__session._deferred_commands_map
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
                    entity = self.__session._documents_by_entity.get(source_entity_or_document_id, None)
                    if not entity:
                        self.throw_not_in_session(source_entity_or_document_id)
                    source_entity_or_document_id = entity.key

                if not isinstance(destination_entity_or_document_id, str):
                    entity = self.__session._documents_by_entity.get(destination_entity_or_document_id, None)
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

                source_document = self.__session._documents_by_id.get_value(source_entity_or_document_id)
                if source_document is not None and source_document.entity in self.__session._deleted_entities:
                    self.__throw_document_already_deleted(
                        source_entity_or_document_id,
                        source_name,
                        "move",
                        destination_entity_or_document_id,
                        source_entity_or_document_id,
                    )

                destination_document = self.__session._documents_by_id.get_value(destination_entity_or_document_id)
                if destination_document is not None and destination_document.entity in self.__session._deleted_entities:
                    self.__throw_document_already_deleted(
                        source_entity_or_document_id,
                        source_name,
                        "move",
                        destination_entity_or_document_id,
                        destination_entity_or_document_id,
                    )

                if (
                    IdTypeAndName.create(source_entity_or_document_id, CommandType.ATTACHMENT_DELETE, source_name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, source_name, "rename", "delete"
                    )

                if (
                    IdTypeAndName.create(source_entity_or_document_id, CommandType.ATTACHMENT_MOVE, source_name)
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, source_name, "rename", "rename"
                    )

                if (
                    IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_DELETE, destination_name
                    )
                    in self.__session._deferred_commands_map
                ):
                    self.__throw_other_deferred_command_exception(
                        source_entity_or_document_id, destination_name, "rename", "delete"
                    )

                if (
                    IdTypeAndName.create(
                        destination_entity_or_document_id, CommandType.ATTACHMENT_MOVE, destination_name
                    )
                    in self.__session._deferred_commands_map
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

                document = self.__session._documents_by_entity.get(entity)
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

        def document_query_from_index_class(
            self, index_class: Type[_TIndex], object_type: Optional[Type[_T]] = None
        ) -> DocumentQuery[_T]:
            if not issubclass(index_class, AbstractCommonApiForIndexes):
                raise TypeError(
                    f"Incorrect type, {index_class} isn't an index. "
                    f"It doesn't inherit from {AbstractCommonApiForIndexes.__name__} "
                )
            index = index_class()
            return self.document_query(object_type, index.index_name, None, index.is_map_reduce)

        def document_query(
            self,
            object_type: Optional[Type[_T]] = None,
            index_name: Union[type, str] = None,
            collection_name: str = None,
            is_map_reduce: bool = False,
        ) -> DocumentQuery[_T]:
            index_name, collection_name = self.session._process_query_parameters(
                object_type, index_name, collection_name, self.session.conventions
            )
            return DocumentQuery(object_type, self, index_name, collection_name, is_map_reduce)


class SessionCountersBase:
    def __init__(self, session: InMemoryDocumentSessionOperations, document_id_or_entity: Union[str, object]):
        if isinstance(document_id_or_entity, str):
            if not document_id_or_entity or document_id_or_entity.isspace():
                raise ValueError("Document id cannot be empty")

            self.doc_id = document_id_or_entity
            self.session = session
        else:
            document = session.documents_by_entity.get(document_id_or_entity)
            if document is None:
                self._throw_entity_not_in_session(document_id_or_entity)
                return
            self.doc_id = document.key
            self.session = session

    def increment(self, counter: str, delta: int = 1) -> None:
        if not counter or counter.isspace():
            raise ValueError("Counter name cannot be empty")

        counter_op = CounterOperation(counter, CounterOperationType.INCREMENT, delta)
        document_info = self.session.documents_by_id.get_value(self.doc_id)
        if document_info is not None and document_info.entity in self.session.deleted_entities:
            self._throw_document_already_deleted_in_session(self.doc_id, counter)

        command = self.session.deferred_commands_map.get(
            IdTypeAndName.create(self.doc_id, CommandType.COUNTERS, None), None
        )
        if command is not None:
            if not isinstance(command, CountersBatchCommandData):
                raise TypeError(f"Command class {command.__class__.name} is invalid")
            counters_batch_command_data: CountersBatchCommandData = command
            if counters_batch_command_data.has_delete(counter):
                self._throw_increment_counter_after_delete_attempt(self.doc_id, counter)

            counters_batch_command_data.counters.operations.append(counter_op)
        else:
            self.session.defer(CountersBatchCommandData(self.doc_id, counter_op))

    def delete(self, counter: str) -> None:
        if not counter or counter.isspace():
            raise ValueError("Counter name is required")

        if IdTypeAndName.create(self.doc_id, CommandType.DELETE, None) in self.session.deferred_commands_map:
            return  # no-op

        document_info = self.session.documents_by_id.get_value(self.doc_id)
        if document_info is not None and document_info.entity in self.session.deleted_entities:
            return  # no-op

        counter_op = CounterOperation(counter, CounterOperationType.DELETE)

        command = self.session.deferred_commands_map.get(
            IdTypeAndName.create(self.doc_id, CommandType.COUNTERS, None), None
        )
        if command is not None:
            counters_batch_command_data: CountersBatchCommandData = command
            if counters_batch_command_data.has_increment(counter):
                self._throw_delete_counter_after_increment_attempt(self.doc_id, counter)

            counters_batch_command_data.counters.operations.append(counter_op)
        else:
            self.session.defer(CountersBatchCommandData(self.doc_id, counter_op))

        cache = self.session.counters_by_doc_id.get(self.doc_id, None)
        if cache is not None:
            del cache[1][counter]

    def _throw_entity_not_in_session(self, entity) -> None:
        raise ValueError(
            "Entity is not associated with the session, cannot add counter to it. "
            "Use document_id instead of track the entity in the session."
        )

    @staticmethod
    def _throw_increment_counter_after_delete_attempt(document_id: str, counter: str) -> None:
        raise ValueError(
            f"Can't increment counter {counter} of document {document_id}, "
            f"there is a deferred command registered to delete a counter with the same name."
        )

    @staticmethod
    def _throw_delete_counter_after_increment_attempt(document_id: str, counter: str) -> None:
        raise ValueError(
            f"Can't delete counter {counter} of document {document_id}, "
            f"there is a deferred command registered to increment a counter with the same name."
        )

    @staticmethod
    def _throw_document_already_deleted_in_session(document_id: str, counter: str) -> None:
        raise ValueError(
            f"Can't increment counter {counter} of document {document_id}, "
            f"the document was already deleted in this session"
        )


class SessionDocumentCounters(SessionCountersBase):
    def get_all(self) -> Dict[str, int]:
        cache = self.session.counters_by_doc_id.get(self.doc_id, None)

        if cache is None:
            cache = [False, CaseInsensitiveDict()]

        missing_counters = not cache[0]

        document = self.session.documents_by_id.get_value(self.doc_id)
        if document is not None:
            metadata_counters: Dict = document.metadata.get(constants.Documents.Metadata.COUNTERS, None)
            if metadata_counters is None:
                missing_counters = False
            elif len(cache[1]) >= len(metadata_counters):
                missing_counters = False

                for c in metadata_counters:
                    if c in cache[1]:
                        continue
                    missing_counters = True
                    break

        if missing_counters:
            # we either don't have the document in session and got_all = False,
            # or we do and cache doesn't contain all metadata counters

            self.session.increment_requests_count()

            details = self.session.operations.send(GetCountersOperation(self.doc_id), self.session.session_info)
            cache[1].clear()

            for counter_detail in details.counters:
                cache[1][counter_detail.counter_name] = counter_detail.total_value

        cache[0] = True

        if not self.session.no_tracking:
            self.session.counters_by_doc_id[self.doc_id] = cache

        return cache[1]

    def get(self, counter) -> int:
        value = None

        cache = self.session.counters_by_doc_id.get(self.doc_id, None)
        if cache is not None:
            value = cache[1].get(counter, None)
            if counter in cache[1]:
                return value
        else:
            cache = [False, CaseInsensitiveDict()]

        document = self.session.documents_by_id.get_value(self.doc_id)
        metadata_has_counter_name = False
        if document is not None:
            metadata_counters = document.metadata.get(constants.Documents.Metadata.COUNTERS, None)
            if metadata_counters is not None:
                for node in metadata_counters:
                    if node.lower() == counter.lower():
                        metadata_has_counter_name = True

        if (document is None and not cache[0]) or metadata_has_counter_name:
            # we either don't have the document in session and got_all = False,
            # or we do and it's metadata contains the counter name

            self.session.increment_requests_count()

            details = self.session.operations.send(
                GetCountersOperation(self.doc_id, counter), self.session.session_info
            )
            if details.counters:
                counter_detail = details.counters[0]
                value = counter_detail.total_value if counter_detail is not None else None

        cache[1][counter] = value
        if not self.session.no_tracking:
            self.session.counters_by_doc_id[self.doc_id] = cache

        return value

    def get_many(self, counters: List[str]) -> Dict[str, int]:
        cache = self.session.counters_by_doc_id.get(self.doc_id, None)
        if cache is None:
            cache = [False, CaseInsensitiveDict()]

        metadata_counters = None
        document = self.session.documents_by_id.get_value(self.doc_id)
        if document is not None:
            metadata_counters = document.metadata.get(constants.Documents.Metadata.COUNTERS, None)

        result = {}

        for counter in counters:
            has_counter = counter in cache[1]
            val = cache[1].get(counter, None)
            not_in_metadata = True

            if document is not None and metadata_counters is not None:
                for metadata_counter in metadata_counters:
                    if metadata_counter.lower() == counter.lower():
                        not_in_metadata = False

            if has_counter or cache[0] or (document is not None and not_in_metadata):
                # we either have value in cache
                # or we have the metadata and the counter is not there,
                # or got_all

                result[counter] = val
                continue

            result.clear()

            self.session.increment_requests_count()

            details = self.session.operations.send(
                GetCountersOperation(self.doc_id, copy.deepcopy(counters), self.session.session_info)
            )

            for counter_detail in details.counters:
                if counter_detail is None:
                    continue

                cache[1][counter_detail.counter_name] = counter_detail.total_value
                result[counter_detail.counter_name] = counter_detail.total_value

            break

        if self.session.no_tracking:
            self.session.counters_by_doc_id[self.doc_id] = cache

        return result


class SessionTimeSeriesBase(abc.ABC):
    def __init__(self, session: InMemoryDocumentSessionOperations, document_id: str, name: str):
        if not document_id or document_id.isspace():
            raise ValueError("Document id cannot be None.")

        if not name or name.isspace():
            raise ValueError("Name cannot be None")

        self.doc_id = document_id
        self.name = name
        self.session = session

    @classmethod
    def from_entity(
        cls, session: InMemoryDocumentSessionOperations, entity: object, name: str
    ) -> SessionTimeSeriesBase:
        if entity is None:
            raise ValueError("Entity cannot be None")

        document_info = session.documents_by_entity.get(entity)

        if document_info is None:
            cls._throw_entity_not_in_session()

        if not name or name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        return cls(session, document_info.key, name)

    @staticmethod
    def _throw_entity_not_in_session() -> None:
        raise ValueError(
            "Entity is not associated with the session, cannot perform time series operations to it. "
            "Use document id instead or track the entity in the session"
        )

    @staticmethod
    def _throw_document_already_deleted_in_session(document_id: str, time_series: str) -> None:
        raise ValueError(
            f"Can't modify time series {time_series} of document {document_id}"
            f", the document was already deleted in this session."
        )

    def append_single(self, timestamp: datetime, value: float, tag: Optional[str] = None) -> None:
        if isinstance(value, int):
            value = float(value)
        if not isinstance(value, float):
            raise TypeError(
                f"Value passed ('{value}') is not a '{float.__name__}'. " f"It is '{value.__class__.__name__}'."
            )

        self.append(timestamp, [value], tag)

    def append(self, timestamp: datetime, values: List[float], tag: Optional[str] = None) -> None:
        if not isinstance(values, List):
            raise TypeError(
                f"The 'values' arg must be a list. If you want to append single float use 'append_single(..)' instead."
            )
        document_info = self.session.documents_by_id.get_value(self.doc_id)
        if document_info is not None and document_info.entity in self.session.deleted_entities:
            self._throw_document_already_deleted_in_session(self.doc_id, self.name)

        op = TimeSeriesOperation.AppendOperation(timestamp, values, tag)
        command = self.session.deferred_commands_map.get(
            IdTypeAndName.create(self.doc_id, CommandType.TIME_SERIES, self.name)
        )
        if command is not None:
            ts_cmd: TimeSeriesBatchCommandData = command
            ts_cmd.time_series.append(op)
        else:
            appends = [op]
            self.session.defer(TimeSeriesBatchCommandData(self.doc_id, self.name, appends, None))

    def delete_all(self) -> None:
        self.delete(None, None)

    def delete_at(self, at: datetime) -> None:
        self.delete(at, at)

    def delete(self, datetime_from: Optional[datetime] = None, datetime_to: Optional[datetime] = None):
        document_info = self.session.documents_by_id.get_value(self.doc_id)
        if document_info is not None and document_info.entity in self.session.deleted_entities:
            self._throw_document_already_deleted_in_session(self.doc_id, self.name)

        op = TimeSeriesOperation.DeleteOperation(datetime_from, datetime_to)

        command = self.session.deferred_commands_map.get(
            IdTypeAndName.create(self.doc_id, CommandType.TIME_SERIES, self.name), None
        )
        if command is not None and isinstance(command, TimeSeriesBatchCommandData):
            ts_cmd: TimeSeriesBatchCommandData = command
            ts_cmd.time_series.delete(op)

        else:
            deletes = [op]
            self.session.defer(TimeSeriesBatchCommandData(self.doc_id, self.name, None, deletes))

    def get_time_series_and_includes(
        self,
        from_datetime: datetime,
        to_datetime: datetime,
        includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]],
        start: int,
        page_size: int,
    ) -> Optional[List[TimeSeriesEntry]]:
        if page_size == 0:
            return []

        document = self.session.documents_by_id.get_value(self.doc_id)
        if document is not None:
            metadata_time_series_raw = document.metadata.get(constants.Documents.Metadata.TIME_SERIES)
            if metadata_time_series_raw is not None and isinstance(metadata_time_series_raw, list):
                time_series = metadata_time_series_raw

                if not any(ts.lower() == self.name.lower() for ts in time_series):
                    # the document is loaded in the session, but the metadata says that there is no such time series
                    return []

        self.session.increment_requests_count()
        range_result: TimeSeriesRangeResult = self.session.operations.send(
            GetTimeSeriesOperation(self.doc_id, self.name, from_datetime, to_datetime, start, page_size, includes),
            self.session.session_info,
        )

        if range_result is None:
            return None

        if not self.session.no_tracking:
            self._handle_includes(range_result)
            needs_write = self.doc_id not in self.session.time_series_by_doc_id
            cache = CaseInsensitiveDict() if needs_write else self.session.time_series_by_doc_id[self.doc_id]

            ranges = cache.get(self.name)
            if ranges is not None and len(ranges) > 0:
                # update
                index = 0 if TSRangeHelper.left(ranges[0].from_date) > TSRangeHelper.right(to_datetime) else len(ranges)
                ranges.insert(index, range_result)
            else:
                item = [range_result]
                cache[self.name] = item

            if needs_write:
                self.session.time_series_by_doc_id[self.doc_id] = cache

        return range_result.entries

    def _handle_includes(self, range_result: TimeSeriesRangeResult) -> None:
        if range_result.includes is None:
            return

        self.session.register_includes(range_result.includes)

        range_result.includes = None

    @staticmethod
    def _skip_and_trim_range_if_needed(
        from_date: datetime,
        to_date: datetime,
        from_range: TimeSeriesRangeResult,
        to_range: TimeSeriesRangeResult,
        values: List[TimeSeriesEntry],
        skip: int,
        trim: int,
    ) -> List[TimeSeriesEntry]:
        if from_range is not None and TSRangeHelper.left(from_date) <= TSRangeHelper.right(from_range.to_date):
            # need to skip a part of the first range
            if to_range is not None and TSRangeHelper.left(to_range.from_date) <= TSRangeHelper.right(to_date):
                # also need to trim a part of the last range
                return values[skip : len(values) - trim]

            return values[skip:]

        if to_range is not None and TSRangeHelper.left(to_range.from_date) <= TSRangeHelper.right(to_date):
            # trim a part of the last range
            return values[: len(values) - trim]

        return values

    def _serve_from_cache(
        self,
        from_date: datetime,
        to_date: datetime,
        start: int,
        page_size: int,
        includes: Callable[[TimeSeriesIncludeBuilder], None],
    ) -> List[TimeSeriesEntry]:
        cache = self.session.time_series_by_doc_id.get(self.doc_id, None)
        ranges = cache.get(self.name)

        # try to find a range in cache that contains [from, to]
        # if found, chop just the relevant part from it and return to the user

        # otherwise, try to find two ranges (from_range, to_range)
        # such that 'from_range' is the last occurrence for which range.from_date <= from_date
        # and to_range is the first occurrence for which range.to_date >= to
        # At the same time, figure out the missing partial ranges that we need to get from the server

        from_range_index = -1
        ranges_to_get_from_server: Optional[List[TimeSeriesRange]] = None
        to_range_index = -1
        cache_includes_whole_range = False

        while True:
            to_range_index += 1
            if to_range_index >= len(ranges):
                break

            if TSRangeHelper.left(ranges[to_range_index].from_date) <= TSRangeHelper.left(from_date):
                if (
                    TSRangeHelper.right(ranges[to_range_index].to_date) >= TSRangeHelper.right(to_date)
                    or len(ranges[to_range_index].entries) - start >= page_size
                ):
                    # we have the entire range in cache
                    # we have all the range we need
                    # or that we have all the results we need in smaller range

                    return self._chop_relevant_range(ranges[to_range_index], from_date, to_date, start, page_size)

                from_range_index = to_range_index
                continue

            # can't get the entire range from cache
            if ranges_to_get_from_server is None:
                ranges_to_get_from_server = []

            # add the missing part [f, t] between current range start (or 'from')
            # and previous range end (or 'to') to the list of ranges we need to get from server

            from_to_use = (
                from_date
                if (
                    to_range_index == 0
                    or TSRangeHelper.right(ranges[to_range_index - 1].to_date) < TSRangeHelper.left(from_date)
                )
                else ranges[to_range_index - 1].to_date
            )
            to_to_use = (
                ranges[to_range_index].from_date
                if TSRangeHelper.left(ranges[to_range_index].from_date) <= TSRangeHelper.right(to_date)
                else to_date
            )

            ranges_to_get_from_server.append(TimeSeriesRange(self.name, from_to_use, to_to_use))

            if TSRangeHelper.right(ranges[to_range_index].to_date) >= TSRangeHelper.right(to_date):
                cache_includes_whole_range = True
                break

        if not cache_includes_whole_range:
            # requested_range [from, to] ends after all ranges in cache
            # add the missing part between the last range end and 'to'
            # to the list of ranges we need to get from server

            if ranges_to_get_from_server is None:
                ranges_to_get_from_server = []

            ranges_to_get_from_server.append(TimeSeriesRange(self.name, ranges[-1].to_date, to_date))

        # get all the missing parts from server

        self.session.increment_requests_count()

        details: TimeSeriesDetails = self.session.operations.send(
            GetMultipleTimeSeriesOperation(self.doc_id, ranges_to_get_from_server, start, page_size, includes),
            self.session.session_info,
        )

        if includes is not None:
            self._register_includes(details)

        # merge all the missing parts as we got from server
        # with all the ranges in cache that are between 'fromRange' and 'toRange'

        merged_values, result_to_user = self._merge_ranges_with_results(
            from_date, to_date, ranges, from_range_index, to_range_index, details.values[self.name]
        )

        if not self.session.no_tracking:
            from_dates = [
                from_date for from_date in [x.from_date for x in details.values.get(self.name)] if from_date is not None
            ]
            from_date = None if not any(from_dates) else min(from_dates)

            to_dates = [
                to_date for to_date in [x.to_date for x in details.values.get(self.name)] if to_date is not None
            ]
            to_date = None if not any(to_dates) else max(to_dates)

            InMemoryDocumentSessionOperations.add_to_cache(
                self.name, from_date, to_date, from_range_index, to_range_index, ranges, cache, merged_values
            )

        return result_to_user

    def _register_includes(self, details: TimeSeriesDetails) -> None:
        for range_result in details.values.get(self.name):
            self._handle_includes(range_result)

    @staticmethod
    def _merge_ranges_with_results(
        from_date: datetime,
        to_date: datetime,
        ranges: List[TimeSeriesRangeResult],
        from_range_index: int,
        to_range_index: int,
        result_from_server: List[TimeSeriesRangeResult],
    ) -> Tuple[List[TimeSeriesEntry], List[TimeSeriesEntry]]:
        skip = 0
        trim = 0
        current_result_index = 0
        merged_values = []

        start = from_range_index if from_range_index != -1 else 0
        end = len(ranges) - 1 if to_range_index == len(ranges) else to_range_index

        for i in range(start, end + 1):
            if i == from_range_index:
                if (
                    TSRangeHelper.left(ranges[i].from_date)
                    <= TSRangeHelper.left(from_date)
                    <= TSRangeHelper.right(ranges[i].to_date)
                ):
                    # requested range [from, to] starts inside 'fromRange'
                    # i.e from_range.from_date <= from_date <= from_range.to_date
                    # so we might need to skip a part of it when we return the
                    # result to the user (i.e. skip [from_range.from_date, from_date])

                    if ranges[i].entries is not None:
                        for v in ranges[i].entries:
                            merged_values.append(v)
                            if v.timestamp < TSRangeHelper.left(from_date):
                                skip += 1
                continue

            if current_result_index < len(result_from_server) and (
                TSRangeHelper.left(result_from_server[current_result_index].from_date)
                < TSRangeHelper.left(ranges[i].from_date)
            ):
                # add current result from server to the merged list
                # in order to avoid duplication, skip first item in range
                # (unless this is the first time we're adding to the merged list)
                to_add = result_from_server[current_result_index].entries[0 if len(merged_values) == 0 else 1 :]
                current_result_index += 1
                merged_values.extend(to_add)

            if i == to_range_index:
                if TSRangeHelper.left(ranges[i].from_date) <= TSRangeHelper.right(to_date):
                    # requested range [from_date, to_date] ends inside to_range
                    # so we might need to trim a part of it when we return the
                    # result to the user (i.e. trim [to_date, to_range.to_date]

                    for index in range(0 if len(merged_values) == 0 else 1, len(ranges[i].entries)):
                        merged_values.append(ranges[i].entries[index])
                        if ranges[i].entries[index].timestamp > TSRangeHelper.right(to_date):
                            trim += 1

                continue

            # add current range from cache to the merged list
            # in order to avoid duplication, skip first item in range if needed

            to_add = ranges[i].entries[0 if len(merged_values) == 0 else 1 :]
            merged_values.extend(to_add)

        if current_result_index < len(result_from_server):
            # the requested range ends after all the ranges in cache,
            # so the last missing part is from server
            # add last missing part to the merged list

            to_add = result_from_server[current_result_index].entries[0 if len(merged_values) == 0 else 1 :]
            current_result_index += 1
            merged_values.extend(to_add)

        result_to_user = SessionTimeSeriesBase._skip_and_trim_range_if_needed(
            from_date,
            to_date,
            None if from_range_index == -1 else ranges[from_range_index],
            None if to_range_index == len(ranges) else ranges[to_range_index],
            merged_values,
            skip,
            trim,
        )

        return merged_values, result_to_user

    @staticmethod
    def _chop_relevant_range(
        ts_range: TimeSeriesRangeResult,
        from_date: datetime,
        to_date: datetime,
        start: int,
        page_size: int,
    ) -> List[TimeSeriesEntry]:
        if ts_range.entries is None:
            return []

        results = []
        for value in ts_range.entries:
            if value.timestamp > TSRangeHelper.right(to_date):
                break

            if value.timestamp < TSRangeHelper.left(from_date):
                continue

            start -= 1
            if start + 1 > 0:
                continue

            page_size -= 1
            if page_size + 1 <= 0:
                break

            results.append(value)

        return results

    def _get_from_cache(
        self,
        from_date: datetime,
        to_date: datetime,
        includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]],
        start: int,
        page_size: int,
    ):
        # RavenDB-16060
        # Typed TimeSeries result need special handling when served from cache
        # since we cache the results untyped

        # in java we return untyped entries here

        result_to_user = self._serve_from_cache(from_date, to_date, start, page_size, includes)
        return result_to_user

    def _not_in_cache(self, from_date: datetime, to_date: datetime):
        cache = self.session.time_series_by_doc_id.get(self.doc_id, None)
        if cache is None:
            return True

        ranges = cache.get(self.name, None)
        if ranges is None:
            return True

        return (
            not ranges
            or TSRangeHelper.left(ranges[0].from_date) > TSRangeHelper.right(to_date)
            or TSRangeHelper.left(from_date) > TSRangeHelper.right(ranges[-1].to_date)
        )

    class CachedEntryInfo:
        def __init__(
            self,
            served_from_cache: bool,
            result_to_user: List[TimeSeriesEntry],
            merged_values: List[TimeSeriesEntry],
            from_range_index: int,
            to_range_index: int,
        ):
            self.served_from_cache = served_from_cache
            self.result_to_user = result_to_user
            self.merged_values = merged_values
            self.from_range_index = from_range_index
            self.to_range_index = to_range_index


class SessionDocumentTimeSeries(SessionTimeSeriesBase):
    def __init__(self, session: InMemoryDocumentSessionOperations, document_id: str, name: str):
        super(SessionDocumentTimeSeries, self).__init__(session, document_id, name)

    @classmethod
    def from_entity(
        cls, session: InMemoryDocumentSessionOperations, entity: object, name: str
    ) -> SessionDocumentTimeSeries:
        if entity is None:
            raise ValueError("Entity cannot be None")

        document_info = session.documents_by_entity.get(entity)

        if document_info is None:
            cls._throw_entity_not_in_session()

        if not name or name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        return cls(session, document_info.key, name)

    def get(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        start: int = 0,
        page_size: int = int_max,
    ) -> Optional[List[TimeSeriesEntry]]:
        return self.get_with_include(from_date, to_date, None, start, page_size)

    def get_with_include(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]] = None,
        start: int = 0,
        page_size: int = int_max,
    ) -> Optional[List[TimeSeriesEntry]]:
        if super()._not_in_cache(from_date, to_date):
            return self.get_time_series_and_includes(from_date, to_date, includes, start, page_size)

        results_to_user = super()._serve_from_cache(from_date, to_date, start, page_size, includes)

        if results_to_user is None:
            return None

        return results_to_user[:page_size]


class SessionDocumentTypedTimeSeries(SessionTimeSeriesBase, Generic[_T_TS_Values_Bindable]):
    def __init__(
        self,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        session: InMemoryDocumentSessionOperations,
        document_id: str,
        name: str,
    ):
        super(SessionDocumentTypedTimeSeries, self).__init__(session, document_id, name)
        self._ts_value_object_type = ts_bindable_object_type

    @classmethod
    def from_entity_typed(
        cls,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        session: InMemoryDocumentSessionOperations,
        entity: object,
        name: str,
    ) -> SessionDocumentTypedTimeSeries:
        if entity is None:
            raise ValueError("Entity cannot be None")

        document_info = session.documents_by_entity.get(entity)

        if document_info is None:
            cls._throw_entity_not_in_session()

        if not name:
            name = TimeSeriesOperations.get_time_series_name(ts_bindable_object_type, session.conventions)
        return cls(ts_bindable_object_type, session, document_info.key, name)

    def get(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        start: int = 0,
        page_size: int = int_max,
    ) -> Optional[List[TypedTimeSeriesEntry[_T_TS_Values_Bindable]]]:
        if super()._not_in_cache(from_date, to_date):
            entries = self.get_time_series_and_includes(from_date, to_date, None, start, page_size)
            if entries is None:
                return None

            return [x.as_typed_entry(self._ts_value_object_type) for x in entries]

        results = self._get_from_cache(from_date, to_date, None, start, page_size)
        return [x.as_typed_entry(self._ts_value_object_type) for x in results]

    def append_single(self, timestamp: datetime, value: _T_TS_Values_Bindable, tag: Optional[str] = None) -> None:
        values = TimeSeriesValuesHelper.get_values(type(value), value)
        super().append(timestamp, values, tag)

    def append_entry(self, entry: TypedTimeSeriesEntry[_T_TS_Values_Bindable]) -> None:
        self.append_single(entry.timestamp, entry.value, entry.tag)


class SessionDocumentRollupTypedTimeSeries(SessionTimeSeriesBase, Generic[_T_TS_Values_Bindable]):
    def __init__(
        self,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        session: InMemoryDocumentSessionOperations,
        document_id: str,
        name: str,
    ):
        super(SessionDocumentRollupTypedTimeSeries, self).__init__(session, document_id, name)
        self._object_type = ts_bindable_object_type

    @classmethod
    def from_entity_rollup_typed(
        cls,
        ts_bindable_object_type: Type[_T_TS_Values_Bindable],
        session: InMemoryDocumentSessionOperations,
        entity: object,
        name: str,
    ) -> SessionDocumentRollupTypedTimeSeries:
        if entity is None:
            raise ValueError("Entity cannot be None")

        document_info = session.documents_by_entity.get(entity)

        if document_info is None:
            cls._throw_entity_not_in_session()

        if not name or name.isspace():
            raise ValueError("Name cannot be None or whitespace")
        return cls(ts_bindable_object_type, session, document_info.key, name)

    def get(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        start: int = 0,
        page_size: int = int_max,
    ) -> List[TypedTimeSeriesRollupEntry[_T_TS_Values_Bindable]]:
        if self._not_in_cache(from_date, to_date):
            results = self.get_time_series_and_includes(from_date, to_date, None, start, page_size)
            return [TypedTimeSeriesRollupEntry.from_entry(self._object_type, x) for x in results]

        results = self._get_from_cache(from_date, to_date, None, start, page_size)
        return [TypedTimeSeriesRollupEntry.from_entry(self._object_type, x) for x in results]

    def append_entry(self, entry: TypedTimeSeriesRollupEntry[_T_TS_Values_Bindable]) -> None:
        values = entry.get_values_from_members()
        super().append(entry.timestamp, values, entry.tag)
