from __future__ import annotations

import datetime
import itertools
import json
from abc import abstractmethod

from ravendb.documents.operations.executor import OperationExecutor
from ravendb.documents.session.misc import (
    SessionOptions,
    TransactionMode,
    SessionInfo,
    ForceRevisionStrategy,
    DocumentsChanges,
)

try:
    from collections.abc import MutableSet
except Exception:
    from collections import MutableSet

import uuid as uuid
from copy import deepcopy, Error
from typing import Optional, Union, Callable, List, Dict, Set, Type, TypeVar, Tuple

from ravendb import constants
from ravendb.documents.commands.crud import GetDocumentsResult
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.identity.hilo import GenerateEntityIdOnTheClient
from ravendb.exceptions.exceptions import NonUniqueObjectException, InvalidOperationException
from ravendb.data.timeseries import TimeSeriesRangeResult
from ravendb.documents.commands.batches import (
    CommandData,
    BatchOptions,
    DeleteCommandData,
    CommandType,
    BatchPatchCommandData,
    PutCommandDataWithJson,
    ForceRevisionCommandData,
    IndexBatchOptions,
    ReplicationBatchOptions,
)
from ravendb.documents.session.cluster_transaction_operation import ClusterTransactionOperationsBase
from ravendb.documents.session.concurrency_check_mode import ConcurrencyCheckMode
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.event_args import *
from ravendb.documents.session.time_series.time_series import TimeSeriesEntry
from ravendb.documents.session.utils.includes_util import IncludesUtil
from ravendb.extensions.json_extensions import JsonExtensions
from ravendb.http.raven_command import RavenCommand
from ravendb.json.json_operation import JsonOperation
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.json.result import BatchCommandResult
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.tools.utils import Utils, CaseInsensitiveDict, CaseInsensitiveSet
from ravendb.documents.store.misc import IdTypeAndName

if TYPE_CHECKING:
    from ravendb.documents.operations.lazy.lazy_operation import LazyOperation
    from ravendb.documents.store.definition import DocumentStore


class RefEq:
    ref = None

    def __init__(self, ref):
        if isinstance(ref, RefEq):
            self.ref = ref.ref
            return
        self.ref = ref

    # As we split the hashable and unhashable items into separate collections, we only compare _RefEq to other _RefEq
    def __eq__(self, other):
        if isinstance(other, RefEq):
            return id(self.ref) == id(other.ref)
        raise TypeError("Expected _RefEq type object")

    def __hash__(self):
        return id(self.ref)


class RefEqEntityHolder(object):
    def __init__(self):
        self.__unhashable_items = dict()

    def __len__(self):
        return len(self.__unhashable_items)

    def __contains__(self, item):
        return RefEq(item) in self.__unhashable_items

    def __delitem__(self, key):
        del self.__unhashable_items[RefEq(key)]

    def __setitem__(self, key, value):
        self.__unhashable_items[RefEq(key)] = value

    def __getitem__(self, key):
        return self.__unhashable_items[RefEq(key)].ref

    def __getattribute__(self, item):
        if item == "_RefEqEntityHolder__unhashable_items":
            return super().__getattribute__(item)
        return self.__unhashable_items.__getattribute__(item)


class DocumentsByIdHolder(object):
    def __init__(self):
        self.__inner = CaseInsensitiveDict()

    def __len__(self):
        return len(self.__inner)

    def __iter__(self):
        return self.__inner.__iter__()

    def __delitem__(self, key):
        del self.__inner[key]

    def __setitem__(self, key, value):
        self.__inner[key] = value

    def __getattribute__(self, item):
        if item in ["_DocumentsByIdHolder__inner", "get_value", "add", "remove", "clear"]:
            return super().__getattribute__(item)
        return self.__inner.__getattribute__(item)

    def get_value(self, key: str) -> DocumentInfo:
        return self.__inner.get(key)

    def add(self, info: DocumentInfo) -> None:
        if info.key in self.__inner:
            return

        self.__inner[info.key] = info

    def remove(self, key: str) -> bool:
        return self.__inner.pop(key, None) is not None

    def clear(self) -> None:
        self.__inner.clear()


class DocumentsByEntityHolder(object):
    def __init__(self):
        self.__documents_by_entity_hashable: Dict[object, DocumentInfo] = dict()
        self.__documents_by_entity_unhashable: RefEqEntityHolder[RefEq, DocumentInfo] = RefEqEntityHolder()
        self.__on_before_store_documents_by_entity_hashable: Dict[object, DocumentInfo] = dict()
        self.__on_before_store_documents_by_entity_unhashable: RefEqEntityHolder[
            RefEq, DocumentInfo
        ] = RefEqEntityHolder()
        self.__prepare_entities_puts: bool = False

    def __repr__(self):
        return f"{self.__class__.__name__}: {[item for item in self]}"

    def __len__(self):
        return (
            len(self.__documents_by_entity_hashable)
            + len(self.__documents_by_entity_unhashable)
            + (
                len(self.__on_before_store_documents_by_entity_hashable)
                if self.__on_before_store_documents_by_entity_hashable
                else 0
            )
            + (
                len(self.__on_before_store_documents_by_entity_unhashable)
                if self.__on_before_store_documents_by_entity_unhashable
                else 0
            )
        )

    def __contains__(self, item):
        if not self.__prepare_entities_puts:
            try:
                return item in self.__documents_by_entity_hashable
            except TypeError as e:
                if str(e.args[0]).startswith("unhashable type"):
                    return item in self.__documents_by_entity_unhashable
                raise e
        self.__create_on_before_store_documents_by_entity_if_needed()
        try:
            return item in self.__on_before_store_documents_by_entity_hashable
        except TypeError as e:
            if str(e.args[0]).startswith("unhashable type"):
                return item in self.__documents_by_entity_unhashable
            raise e

    def __setitem__(self, key, value):
        if not self.__prepare_entities_puts:
            try:
                self.__documents_by_entity_hashable[key] = value
            except TypeError as e:
                if str(e.args[0]).startswith("unhashable type"):
                    self.__documents_by_entity_unhashable[key] = value
                    return
                raise e
            return
        self.__create_on_before_store_documents_by_entity_if_needed()
        try:
            self.__on_before_store_documents_by_entity_hashable[key] = value
        except TypeError as e:
            if str(e.args[0]).startswith("unhashable type"):
                self.__on_before_store_documents_by_entity_unhashable[key] = value
                return
            raise e

    def __create_on_before_store_documents_by_entity_if_needed(self) -> None:
        if not self.__on_before_store_documents_by_entity_unhashable:
            self.__on_before_store_documents_by_entity_unhashable = RefEqEntityHolder()
        if not self.__on_before_store_documents_by_entity_hashable:
            self.__on_before_store_documents_by_entity_hashable = dict()
        return

    def __getitem__(self, key):
        try:
            document_info = self.__documents_by_entity_hashable[key]
        except (TypeError, KeyError):
            document_info = self.__documents_by_entity_unhashable.get(RefEq(key), None)
        if document_info:
            return document_info
        if self.__on_before_store_documents_by_entity_hashable:
            document_info = self.__on_before_store_documents_by_entity_hashable.get(key, None)
        if not document_info and self.__documents_by_entity_unhashable:
            return self.__on_before_store_documents_by_entity_unhashable.get(RefEq(key), None)
        return None

    def __iter__(self):
        first_hashable = (
            (
                self.DocumentsByEntityEnumeratorResult(key, value, True)
                for key, value in self.__documents_by_entity_hashable.items()
            )
            if len(self.__documents_by_entity_hashable) > 0
            else None
        )

        first_unhashable = (
            (
                self.DocumentsByEntityEnumeratorResult(key, value, True)
                for key, value in self.__documents_by_entity_unhashable.items()
            )
            if len(self.__documents_by_entity_unhashable) > 0
            else None
        )

        second_hashable = (
            (
                self.DocumentsByEntityEnumeratorResult(key, value, False)
                for key, value in self.__on_before_store_documents_by_entity_hashable.items()
            )
            if len(self.__on_before_store_documents_by_entity_hashable) > 0
            else None
        )
        second_unhashable = (
            (
                self.DocumentsByEntityEnumeratorResult(key, value, False)
                for key, value in self.__on_before_store_documents_by_entity_unhashable.items()
            )
            if len(self.__on_before_store_documents_by_entity_unhashable) > 0
            else None
        )

        return itertools.chain(
            *[x for x in [first_hashable, first_unhashable, second_hashable, second_unhashable] if x]
        )

    def get(self, key, default=None):
        return self[key] if key in self else default

    def pop(self, key, default_value=None):
        result = self.__documents_by_entity_hashable.pop(key, None)
        if not result:
            result = self.__documents_by_entity_unhashable.pop(RefEq(key), default_value)

        if self.__on_before_store_documents_by_entity_unhashable:
            if not result:
                result = self.__on_before_store_documents_by_entity_unhashable.pop(RefEq(key), None)
            else:
                del self.__on_before_store_documents_by_entity_unhashable[RefEq(key)]
        if self.__on_before_store_documents_by_entity_hashable:
            if not result:
                result = self.__on_before_store_documents_by_entity_hashable.pop(key, None)
            else:
                del self.__on_before_store_documents_by_entity_hashable

        return result

    def evict(self, entity: object) -> None:
        if self.__prepare_entities_puts:
            raise RuntimeError("Cannot evict entity during on_before_store")
        result = self.__documents_by_entity_unhashable.pop(RefEq(entity), None)
        if result is None:
            self.__documents_by_entity_hashable.pop(entity, None)

    def clear(self):
        self.__documents_by_entity_hashable.clear()
        self.__documents_by_entity_unhashable.clear()
        if self.__on_before_store_documents_by_entity_hashable:
            self.__on_before_store_documents_by_entity_hashable.clear()
        if self.__on_before_store_documents_by_entity_unhashable:
            self.__on_before_store_documents_by_entity_unhashable.clear()

    class DocumentsByEntityEnumeratorResult:
        def __init__(self, key: object, value: DocumentInfo, execute_on_before_store: bool):
            self.__key = key.ref if isinstance(key, RefEq) else key
            self.__value = value
            self.__execute_on_before_store = execute_on_before_store

        @property
        def key(self):
            return self.__key

        @property
        def value(self):
            return self.__value

        @property
        def execute_on_before_store(self):
            return self.__execute_on_before_store


class DeletedEntitiesHolder(MutableSet):
    def __init__(self, items=None, prepare_entities_deletes: bool = False):
        if items is None:
            items = []
        self.__deleted_entities = set(map(RefEq, items))
        self.__prepare_entities_deleted = prepare_entities_deletes
        self.__on_before_deleted_entities: Union[set, None] = None

    def __getattribute__(self, item):
        if item in [
            "add",
            "remove",
            "evict",
            "clear",
            "discard",
            "DeletedEntitiesEnumeratorResult",
            "_DeletedEntitiesHolder__deleted_entities",
            "_DeletedEntitiesHolder__prepare_entities_deleted",
            "_DeletedEntitiesHolder__on_before_deleted_entities",
        ]:
            return super().__getattribute__(item)
        return self.__deleted_entities.__getattribute__(item)  # todo: getattribute of joined sets, not just one..

    def __contains__(self, item: object) -> bool:
        if RefEq(item) in self.__deleted_entities:
            return True
        if self.__on_before_deleted_entities is None:
            return False
        return RefEq(item) in self.__on_before_deleted_entities

    def __len__(self) -> int:
        return len(self.__deleted_entities) + (
            len(self.__on_before_deleted_entities) if self.__on_before_deleted_entities else 0
        )

    def __iter__(self):
        deleted_transformed_iterator = (
            self.DeletedEntitiesEnumeratorResult(item.ref, True) for item in self.__deleted_entities
        )
        if self.__on_before_deleted_entities is None:
            return deleted_transformed_iterator

        on_before_deleted_iterator = (
            self.DeletedEntitiesEnumeratorResult(item.ref, False) for item in self.__on_before_deleted_entities
        )
        return itertools.chain(deleted_transformed_iterator, on_before_deleted_iterator)

    def add(self, element: object) -> None:
        if self.__prepare_entities_deleted:
            if self.__on_before_deleted_entities is None:
                self.__on_before_deleted_entities = set()
            self.__on_before_deleted_entities.add(RefEq(element))
            return
        return self.__deleted_entities.add(RefEq(element))

    def remove(self, entity: object) -> None:
        self.__deleted_entities.remove(RefEq(entity))
        if self.__on_before_deleted_entities:
            self.__on_before_deleted_entities.remove(RefEq(entity))

    def discard(self, element: object) -> None:
        self.__deleted_entities.discard(RefEq(element))
        if self.__on_before_deleted_entities is not None:
            self.__on_before_deleted_entities.remove(RefEq(element))

    def clear(self) -> None:
        self.__deleted_entities.clear()
        if self.__on_before_deleted_entities:
            self.__on_before_deleted_entities.clear()

    def evict(self, entity) -> None:
        if self.__prepare_entities_deleted:
            raise RuntimeError("Cannot evict entity during OnBeforeDelete")
        self.__deleted_entities.remove(RefEq(entity))

    class DeletedEntitiesEnumeratorResult:
        def __init__(self, entity: object, execute_on_before_delete: bool):
            self.__entity = entity
            self.__execute_on_before_delete = execute_on_before_delete

        @property
        def entity(self) -> object:
            return self.__entity

        @property
        def execute_on_before_delete(self) -> bool:
            return self.__execute_on_before_delete


_T = TypeVar("_T")


class InMemoryDocumentSessionOperations:
    __instances_counter: int = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __init__(self, store: "DocumentStore", key: uuid.UUID, options: SessionOptions):
        self.__id = key
        self.__database_name = options.database if options.database else store.database if store.database else None
        if not self.__database_name:
            InMemoryDocumentSessionOperations.raise_no_database()

        self._request_executor = (
            options.request_executor if options.request_executor else store.get_request_executor(self.__database_name)
        )
        self._operation_executor: Union[None, OperationExecutor] = None

        self._pending_lazy_operations: List[LazyOperation] = []
        self._on_evaluate_lazy = {}

        self.__no_tracking = options.no_tracking

        self.__use_optimistic_concurrency = self._request_executor.conventions.use_optimistic_concurrency
        self.__max_number_of_requests_per_session = (
            self._request_executor.conventions.max_number_of_requests_per_session
        )
        self.__generate_entity_id_on_client = GenerateEntityIdOnTheClient(
            self._request_executor.conventions, self._generate_id
        )
        self.entity_to_json = EntityToJson(self)
        self._document_store: DocumentStore = store
        self.session_info = SessionInfo(self, options, self._document_store)
        self.__save_changes_options = BatchOptions()

        self.transaction_mode = options.transaction_mode
        self.disable_atomic_document_writes_in_cluster_wide_transaction = (
            options.disable_atomic_document_writes_in_cluster_wide_transaction
        )

        self._known_missing_ids = CaseInsensitiveSet()
        self._documents_by_id = DocumentsByIdHolder()
        self._included_documents_by_id = CaseInsensitiveDict()
        self._documents_by_entity: DocumentsByEntityHolder = DocumentsByEntityHolder()

        self._counters_by_doc_id: Dict[str, List[bool, Dict[str, int]]] = {}
        self._time_series_by_doc_id: Dict[str, Dict[str, List[TimeSeriesRangeResult]]] = {}

        self._deleted_entities: Union[
            Set[DeletedEntitiesHolder.DeletedEntitiesEnumeratorResult], DeletedEntitiesHolder
        ] = DeletedEntitiesHolder()
        self._deferred_commands: List[CommandData] = []
        self._deferred_commands_map: Dict[IdTypeAndName, CommandData] = {}
        self._ids_for_creating_forced_revisions: Dict[str, ForceRevisionStrategy] = CaseInsensitiveDict()
        # todo: pendingLazyOperations, onEvaluateLazy
        self._generate_document_keys_on_store: bool = True
        self._save_changes_options: Union[None, BatchOptions] = None
        self.__hash: int = self.__instances_counter.__add__(1)
        self.__is_disposed: Union[None, bool] = None

        self.__number_of_requests: int = 0
        self.__max_number_of_requests_per_session: int = (
            self._request_executor.conventions.max_number_of_requests_per_session
        )

        # --- EVENTS ---
        self.__before_store: List[Callable[[BeforeStoreEventArgs], None]] = []
        self.__after_save_changes: List[Callable[[AfterSaveChangesEventArgs], None]] = []
        self.__before_delete: List[Callable[[BeforeDeleteEventArgs], None]] = []
        self.__before_query: List[Callable[[BeforeQueryEventArgs], None]] = []
        self.__before_conversion_to_document: List[Callable[[BeforeConversionToDocumentEventArgs], None]] = []
        self.__after_conversion_to_document: List[Callable[[AfterConversionToDocumentEventArgs], None]] = []

        self.__before_conversion_to_entity: List[Callable[[BeforeConversionToEntityEventArgs], None]] = []

        self.__after_conversion_to_entity: List[Callable[[AfterConversionToEntityEventArgs], None]] = []

        self.__session_closing: List[Callable[[SessionClosingEventArgs], None]] = []

    def add_before_store(self, event: Callable[[BeforeStoreEventArgs], None]):
        self.__before_store.append(event)

    def remove_before_store(self, event: Callable[[BeforeStoreEventArgs], None]):
        self.__before_store.remove(event)

    def add_session_closing(self, event: Callable[[SessionClosingEventArgs], None]):
        self.__session_closing.append(event)

    def remove_session_closing(self, event: Callable[[SessionClosingEventArgs], None]):
        self.__session_closing.remove(event)

    def add_before_conversion_to_document(self, event: Callable[[BeforeConversionToDocumentEventArgs], None]):
        self.__before_conversion_to_document.append(event)

    def remove_before_conversion_to_document(self, event: Callable[[BeforeConversionToDocumentEventArgs], None]):
        self.__before_conversion_to_document.remove(event)

    def add_after_conversion_to_document(self, event: Callable[[AfterConversionToDocumentEventArgs], None]):
        self.__after_conversion_to_document.append(event)

    def remove_after_conversion_to_document(self, event: Callable[[AfterConversionToDocumentEventArgs], None]):
        self.__after_conversion_to_document.remove(event)

    def add_before_conversion_to_entity(self, event: Callable[[BeforeConversionToEntityEventArgs], None]):
        self.__before_conversion_to_entity.append(event)

    def remove_before_conversion_to_entity(self, event: Callable[[BeforeConversionToEntityEventArgs], None]):
        self.__before_conversion_to_entity.remove(event)

    def add_after_conversion_to_entity(self, event: Callable[[AfterConversionToEntityEventArgs], None]):
        self.__after_conversion_to_entity.append(event)

    def remove_after_conversion_to_entity(self, event: Callable[[AfterConversionToEntityEventArgs], None]):
        self.__after_conversion_to_entity.remove(event)

    def add_after_save_changes(self, event: Callable[[AfterSaveChangesEventArgs], None]):
        self.__after_save_changes.append(event)

    def remove_after_save_changes(self, event: Callable[[AfterSaveChangesEventArgs], None]):
        self.__after_save_changes.remove(event)

    def add_before_delete(self, event: Callable[[BeforeDeleteEventArgs], None]):
        self.__before_delete.append(event)

    def remove_before_delete_entity(self, event: Callable[[BeforeDeleteEventArgs], None]):
        self.__before_delete.remove(event)

    def add_before_query(self, event: Callable[[BeforeQueryEventArgs], None]):
        self.__before_query.append(event)

    def remove_before_query(self, event: Callable[[BeforeQueryEventArgs], None]):
        self.__before_query.append(event)

    def before_store_invoke(self, before_store_event_args: BeforeStoreEventArgs):
        for event in self.__before_store:
            event(before_store_event_args)

    def session_closing_invoke(self, session_closing_event_args: SessionClosingEventArgs):
        for event in self.__session_closing:
            event(session_closing_event_args)

    def before_conversion_to_document_invoke(
        self, before_conversion_to_document_event_args: BeforeConversionToDocumentEventArgs
    ):
        for event in self.__before_conversion_to_document:
            event(before_conversion_to_document_event_args)

    def after_conversion_to_document_invoke(
        self, after_conversion_to_document_event_args: AfterConversionToDocumentEventArgs
    ):
        for event in self.__after_conversion_to_document:
            event(after_conversion_to_document_event_args)

    def before_conversion_to_entity_invoke(
        self, before_conversion_to_entity_event_args: BeforeConversionToEntityEventArgs
    ):
        for event in self.__before_conversion_to_entity:
            event(before_conversion_to_entity_event_args)

    def after_conversion_to_entity_invoke(
        self, after_conversion_to_entity_event_args: AfterConversionToEntityEventArgs
    ):
        for event in self.__after_conversion_to_entity:
            event(after_conversion_to_entity_event_args)

    def after_save_changes_invoke(self, after_save_changes_event_args: AfterSaveChangesEventArgs):
        for event in self.__after_save_changes:
            event(after_save_changes_event_args)

    def before_delete_invoke(self, before_delete_event_args: BeforeDeleteEventArgs):
        for event in self.__before_delete:
            event(before_delete_event_args)

    def before_query_invoke(self, before_query_event_args: BeforeQueryEventArgs):
        for event in self.__before_query:
            event(before_query_event_args)

    @property
    def operation_executor(self) -> OperationExecutor:
        return self._operation_executor

    @property
    def conventions(self) -> DocumentConventions:
        return self._request_executor.conventions

    @property
    def _store_identifier(self):
        return self._document_store.identifier

    @property
    def database_name(self):
        return self.__database_name

    @property
    def generate_entity_id_on_the_client(self) -> GenerateEntityIdOnTheClient:
        return self.__generate_entity_id_on_client

    @property
    def counters_by_doc_id(self):
        if self._counters_by_doc_id is None:
            self._counters_by_doc_id = CaseInsensitiveDict()
        return self._counters_by_doc_id

    @property
    def number_of_requests(self) -> int:
        return self.__number_of_requests

    @property
    def no_tracking(self) -> bool:
        return self.__no_tracking

    @no_tracking.setter
    def no_tracking(self, value: bool):
        self.__no_tracking = value

    # def counters_for(self, entity_or_document_id):
    #     """
    #     Get A counters object associated with the document
    #     """
    #     return ravendb.documents.DocumentCounters(self, entity_or_document_id)

    @staticmethod
    def raise_no_database() -> None:
        raise RuntimeError(
            "Cannot open a Session without specyfing a name of a database "
            "to operate on. Database name can be passed as an argument when Session is"
            " being opened or default database can be defined using 'DocumentStore.database' field"
        )

    @abstractmethod
    def _generate_id(self, entity: object) -> str:
        pass

    def _get_document_info(self, entity: object) -> DocumentInfo:
        document_info = self._documents_by_entity.get(entity)
        if document_info:
            return document_info

        found, value = self.__generate_entity_id_on_client.try_get_id_from_instance(entity)
        if not found:
            raise ValueError(f"Could not find the document id for {entity}")
        self._assert_no_non_unique_instance(entity, value)
        raise ValueError(f"Document {value} doesn't exist in the session")

    def is_loaded(self, key: str) -> bool:
        return self.is_loaded_or_deleted(key)

    def is_loaded_or_deleted(self, key: str) -> bool:
        document_info = self._documents_by_id.get(key)
        return (
            document_info is not None
            and (document_info.document is not None or document_info.entity is not None)
            or self.is_deleted(key)
            or key in self._included_documents_by_id
        )

    def is_deleted(self, key: str) -> bool:
        return key in self._known_missing_ids

    def increment_requests_count(self) -> None:
        self.__number_of_requests += 1
        if self.__number_of_requests > self.__max_number_of_requests_per_session:
            raise ValueError(
                f"The maximum number of requests {self.__max_number_of_requests_per_session} allowed for this session"
                "has been reached. "
                "Raven limits the number of remote calls that a session is allowed to make as an early warning system. "
                "Sessions are expected to be short lived, and Raven provides facilities like load(String[] keys) "
                "to load multiple documents at once and batch saves (call SaveChanges() only once). "
                "You can increase the limit by setting DocumentConvention.MaxNumberOfRequestsPerSession or "
                "MaxNumberOfRequestsPerSession, but it is advisable that you'll look into reducing the number of remote"
                " calls first, since that will speed up your application significantly"
                "and result in a more responsive application."
            )

    def _assert_no_non_unique_instance(self, entity: object, key: str) -> None:
        if (not key) or key[-1] == "|" or key[-1] == self.conventions.identity_parts_separator:
            return

        info = self._documents_by_id.get(key)
        if info is None or info.entity is entity:
            return

        raise NonUniqueObjectException(f"Attempted to associate a different object with id '{key}'.")

    def track_entity_document_info(self, entity_type: Type[_T], document_found: DocumentInfo) -> _T:
        return self.track_entity(
            entity_type, document_found.key, document_found.document, document_found.metadata, self.no_tracking
        )

    def register_external_loaded_into_the_session(self, info: DocumentInfo) -> None:
        if self.no_tracking:
            return

        existing = self._documents_by_id.get_value(info.key)
        if existing is not None:
            if existing.entity == info.entity:
                return

            raise RuntimeError(f"The document {info.key} is already in the session with a different entity instance.")

        existing_entity = self._documents_by_entity.get(info.entity)
        if existing_entity is not None:
            if existing_entity.key.lower() == info.key.lower():
                return

            raise RuntimeError(
                f"Attempted to load an entity with id {info.key}, "
                f"but the entity instance already exists in the session with id: {existing_entity.key}"
            )

        self._documents_by_entity[info.entity] = info
        self._documents_by_id.add(info)
        self._included_documents_by_id.remove(info.key)

    def track_entity(
        self,
        entity_type: Type[_T],
        key: Optional[str],
        document: dict,
        metadata: dict,
        no_tracking: bool,
    ) -> _T:
        no_tracking = self.no_tracking or no_tracking
        if not key:
            return self.__deserialize_from_transformer(entity_type, None, document, False)

        doc_info = self._documents_by_id.get(key)
        if doc_info is not None:
            if doc_info.entity is None:
                doc_info.entity = self.entity_to_json.convert_to_entity(entity_type, key, document, not no_tracking)

            if not no_tracking:
                self._included_documents_by_id.pop(key, None)
                self._documents_by_entity[doc_info.entity] = doc_info

            return doc_info.entity

        doc_info = self._included_documents_by_id.get(key)
        if doc_info:
            if doc_info.entity is None:
                doc_info.entity = self.entity_to_json.convert_to_entity(entity_type, key, document, not no_tracking)

            if not no_tracking:
                self._included_documents_by_id.pop(key, None)
                self._documents_by_id[doc_info.key] = doc_info
                self._documents_by_entity[doc_info.entity] = doc_info

            return doc_info.entity

        entity = self.entity_to_json.convert_to_entity(entity_type, key, document, not no_tracking)

        change_vector = metadata.get(constants.Documents.Metadata.CHANGE_VECTOR)
        if change_vector is None:
            raise ValueError(f"Document {key} must have a Change Vector")

        if not no_tracking:
            new_document_info = DocumentInfo(
                key=key, document=document, metadata=metadata, entity=entity, change_vector=change_vector
            )
            self._documents_by_id[new_document_info.key] = new_document_info
            self._documents_by_entity[new_document_info.entity] = new_document_info

        return entity

    def delete(self, key_or_entity: Union[str, object], expected_change_vector: Optional[str] = None) -> None:
        if isinstance(key_or_entity, str):
            key = key_or_entity
            if key is None:
                raise ValueError("Id cannot be None")
            change_vector = None
            document_info = self._documents_by_id.get(key)
            if document_info is not None:
                new_obj = self.entity_to_json.convert_entity_to_json(document_info.entity, document_info)
                if document_info.entity is not None and self._entity_changed(new_obj, document_info, None):
                    raise InvalidOperationException(
                        "Can't delete changed entity using identifier. Use delete(entity) instead."
                    )

                if document_info.entity is not None:
                    self._documents_by_entity.pop(document_info.entity, None)
                self._documents_by_id.pop(key, None)
                change_vector = document_info.change_vector

            self._known_missing_ids.add(key)
            change_vector = change_vector if self.__use_optimistic_concurrency else None
            if self._counters_by_doc_id:
                self._counters_by_doc_id.pop(key, None)
            self._defer(DeleteCommandData(key, expected_change_vector if expected_change_vector else change_vector))
            return

        entity = key_or_entity
        if entity is None:
            raise ValueError("Entity cannot be None")

        value = self._documents_by_entity.get(entity)
        if value is None:
            raise InvalidOperationException(
                f"{entity} is not associated with the session, cannot delete unknown entity instance."
            )

        self._deleted_entities.add(entity)
        self._included_documents_by_id.pop(value.key, None)
        if self._counters_by_doc_id:
            self._counters_by_doc_id.pop(value.key, None)
        self._known_missing_ids.add(value.key)

    def store(self, entity: object, key: Optional[str] = None, change_vector: Optional[str] = None) -> None:
        if all([entity, not key, not change_vector]):
            has_id = self.__generate_entity_id_on_client.try_get_id_from_instance(entity)[0]
            checkmode = ConcurrencyCheckMode.FORCED if not has_id else ConcurrencyCheckMode.AUTO
        elif all([entity, key, not change_vector]):
            checkmode = ConcurrencyCheckMode.AUTO
        elif all([entity, key, change_vector]):
            checkmode = ConcurrencyCheckMode.DISABLED if change_vector is None else ConcurrencyCheckMode.FORCED
        else:
            raise ValueError("Unexpected arguments set. Pass entity / entity and key / entity,key and change_vector.")
        self.__store_internal(entity, change_vector, key, checkmode)

    def __store_internal(
        self, entity: object, change_vector: str, key: str, force_concurrency_check: ConcurrencyCheckMode
    ):
        if self.no_tracking:
            raise RuntimeError("Cannot legacy entity. Entity tracking is disabled in this session.")
        if entity is None:
            raise ValueError("Entity cannot be None")

        value = self._documents_by_entity.get(entity)
        if value is not None:
            value.change_vector = change_vector if change_vector else value.change_vector
            value.concurrency_check_mode = force_concurrency_check
            return

        if key is None:
            if self._generate_document_keys_on_store:
                key = self.__generate_entity_id_on_client.generate_document_key_for_storage(entity)
            else:
                self._remember_entity_for_document_id_generation(entity)
        else:
            self.__generate_entity_id_on_client.try_set_identity(entity, key)

        if IdTypeAndName.create(key, CommandType.CLIENT_ANY_COMMAND, None) in self._deferred_commands_map.keys():
            raise InvalidOperationException(
                f"Can't legacy document, there is a deferred command registered for this document in the session. "
                f"Document id:{key}"
            )

        if entity in self._deleted_entities:
            raise RuntimeError(f"Can't store object, it was already deleted in this session. Document id {key}")

        self._assert_no_non_unique_instance(entity, key)

        collection_name = self._request_executor.conventions.get_collection_name(entity)
        metadata = {}
        if collection_name:
            metadata[constants.Documents.Metadata.COLLECTION] = collection_name
        python_type = self._request_executor.conventions.find_python_class_name(type(entity))
        if python_type:
            metadata[constants.Documents.Metadata.RAVEN_PYTHON_TYPE] = python_type
        if key:
            self._known_missing_ids.discard(key)
        self._store_entity_in_unit_of_work(key, entity, change_vector, metadata, force_concurrency_check)

    def _remember_entity_for_document_id_generation(self, entity: object) -> None:
        raise NotImplementedError(
            "You cannot set generate_document_ids_on_store to false without "
            "implementing remember_entity_for_document_id_generation"
        )
        pass

    def _store_entity_in_unit_of_work(
        self,
        key: str,
        entity: object,
        change_vector: str,
        metadata: dict,
        force_concurrency_check: ConcurrencyCheckMode,
    ) -> None:
        if key:
            self._known_missing_ids.discard(key)

        document_info = DocumentInfo(
            key=key,
            metadata=metadata,
            change_vector=change_vector,
            concurrency_check_mode=force_concurrency_check,
            entity=entity,
            new_document=True,
            document=None,
        )
        self._documents_by_entity[entity] = document_info
        if key:
            self._documents_by_id[key] = document_info

    def prepare_for_save_changes(self) -> SaveChangesData:
        result = InMemoryDocumentSessionOperations.SaveChangesData(self)
        deferred_commands_count = len(self._deferred_commands)

        self.__prepare_for_entities_deletion(result, None)
        self.__prepare_for_entities_puts(result)
        self.__prepare_for_creating_revisions_from_ids(result)
        self.__prepare_compare_exchange_entities(result)

        if len(self._deferred_commands) > deferred_commands_count:
            # this allow on_before_store to call defer during the call to include
            # additional values during the same SaveChanges call

            result.deferred_commands.extend(self._deferred_commands)
            result.deferred_commands_map.update(self._deferred_commands_map)

        for deferred_command in result.deferred_commands:
            if deferred_command.on_before_save_changes:
                deferred_command.on_before_save_changes(self)

        return result

    def validate_cluster_transaction(self, result: SaveChangesData) -> None:
        if self.transaction_mode != TransactionMode.CLUSTER_WIDE:
            return

        if self.__use_optimistic_concurrency:
            raise RuntimeError(
                f"useOptimisticConcurrency is not supported with TransactionMode set to {TransactionMode.CLUSTER_WIDE}"
            )

        for command_data in result.session_commands:
            if command_data.command_type == CommandType.PUT or CommandType.DELETE:
                if command_data.change_vector is not None:
                    raise ValueError(
                        f"Optimistic concurrency for {command_data.key} "
                        f"is not supported when using a cluster transaction"
                    )
            elif command_data.command_type == CommandType.COMPARE_EXCHANGE_DELETE or CommandType.COMPARE_EXCHANGE_PUT:
                pass
            else:
                raise ValueError(f"The command '{command_data.command_type}' is not supported in a cluster session.")

    def __prepare_compare_exchange_entities(self, result: SaveChangesData) -> None:
        if not self._has_cluster_session():
            return

        cluster_transaction_operations = self.cluster_transaction
        if cluster_transaction_operations.number_of_tracked_compare_exchange_values == 0:
            return

        if self.transaction_mode != TransactionMode.CLUSTER_WIDE:
            raise RuntimeError(
                "Performing cluster transaction operation require the TransactionMode to be set to CLUSTER_WIDE"
            )

        self.cluster_transaction.prepare_compare_exchange_entities(result)

    # --- CLUSTER ---
    @abstractmethod
    def _has_cluster_session(self) -> bool:
        pass

    @abstractmethod
    def _clear_cluster_session(self) -> None:
        pass

    @property
    @abstractmethod
    def cluster_transaction(self) -> ClusterTransactionOperationsBase:
        raise RuntimeError(f"{self.__class__.__name__} must override the cluster_session property method")

    @staticmethod
    def __update_metadata_modifications(document_info: DocumentInfo) -> bool:
        dirty = False
        if document_info.metadata_instance is not None:
            if document_info.metadata_instance.is_dirty:
                dirty = True
            for key, value in document_info.metadata_instance.items():
                if value is None or isinstance(value, MetadataAsDictionary) and value.is_dirty is True:
                    dirty = True
                document_info.metadata[key] = json.loads(json.dumps(value, default=Utils.json_default))
        return dirty

    def __prepare_for_creating_revisions_from_ids(self, result: SaveChangesData) -> None:
        for id_entry in self._ids_for_creating_forced_revisions:
            result.session_commands.append(ForceRevisionCommandData(id_entry))

        self._ids_for_creating_forced_revisions.clear()

    def __prepare_for_entities_deletion(
        self, result: Union[None, SaveChangesData], changes: Union[None, Dict[str, List[DocumentsChanges]]]
    ) -> None:
        for deleted_entity in self._deleted_entities:
            document_info = self._documents_by_entity.get(deleted_entity.entity)
            if document_info is None:
                continue
            if changes is not None:
                doc_changes = []
                change = DocumentsChanges("", "", DocumentsChanges.ChangeType.DOCUMENT_DELETED)
                doc_changes.append(change)
                changes[document_info.key] = doc_changes
            else:
                command = result.deferred_commands_map.get(
                    IdTypeAndName.create(document_info.key, CommandType.CLIENT_ANY_COMMAND, None)
                )
                if command:
                    self.__throw_invalid_deleted_document_with_deferred_command(command)

                change_vector = None
                document_info = self._documents_by_id.get(document_info.key)

                if document_info:
                    change_vector = document_info.change_vector

                    if document_info.entity is not None:
                        result.on_success.remove_document_by_entity(document_info.entity)
                        result.entities.append(document_info.entity)

                    result.on_success.remove_document_by_entity(document_info.key)

                change_vector = change_vector if self.__use_optimistic_concurrency else None
                self.before_delete_invoke(BeforeDeleteEventArgs(self, document_info.key, document_info.entity))
                result.session_commands.append(DeleteCommandData(document_info.key, document_info.change_vector))

            if changes is None:
                result.on_success.clear_deleted_entities()

    def __prepare_for_entities_puts(self, result: SaveChangesData) -> None:
        should_ignore_entity_changes = self.conventions.should_ignore_entity_changes
        for entity in self._documents_by_entity:
            entity: DocumentsByEntityHolder.DocumentsByEntityEnumeratorResult
            if entity.value.ignore_changes:
                continue

            if should_ignore_entity_changes:
                if should_ignore_entity_changes.check(self, entity.value.entity, entity.value.key):
                    continue
            if self.is_deleted(entity.value.key):
                continue

            dirty_metadata = self.__update_metadata_modifications(entity.value)

            document = self.entity_to_json.convert_entity_to_json(entity.key, entity.value)

            if not self._entity_changed(document, entity.value, None) and not dirty_metadata:
                continue

            command = result.deferred_commands_map.get(
                IdTypeAndName.create(entity.value.key, CommandType.CLIENT_MODIFY_DOCUMENT_COMMAND, None),
                None,
            )
            if command:
                self.__throw_invalid_modified_document_with_deferred_command(command)

            if self.__before_store and entity.execute_on_before_store:
                before_store_event_args = BeforeStoreEventArgs(self, entity.value.key, entity.key)
                self.before_store_invoke(before_store_event_args)

                if before_store_event_args.is_metadata_accessed:
                    self.__update_metadata_modifications(entity.value)

                if before_store_event_args.is_metadata_accessed or self._entity_changed(document, entity.value, None):
                    document = self.entity_to_json.convert_entity_to_json(entity.key, entity.value)

            result.entities.append(entity.key)

            if entity.value.key is not None:
                result.on_success.remove_document_by_id(entity.value.key)

            result.on_success.update_entity_document_info(entity.value, document)

            change_vector = (
                (entity.value.change_vector if entity.value.change_vector else "")
                if self.__use_optimistic_concurrency
                else (
                    entity.value.change_vector
                    if entity.value.concurrency_check_mode == ConcurrencyCheckMode.FORCED
                    else None
                )
            )

            force_revision_creation_strategy = ForceRevisionStrategy.NONE
            if entity.value.key is not None:
                creation_strategy = self._ids_for_creating_forced_revisions.get(entity.value.key)
                if creation_strategy is not None:
                    self._ids_for_creating_forced_revisions.pop(entity.value.key, None)
                    force_revision_creation_strategy = creation_strategy

            result.session_commands.append(
                PutCommandDataWithJson(entity.value.key, change_vector, document, force_revision_creation_strategy)
            )

    @staticmethod
    def __throw_invalid_modified_document_with_deferred_command(result_command: CommandData) -> None:
        raise RuntimeError(
            f"Cannot perform save because document {result_command.key} has been modified by "
            f"the session and is also taking part in deferred {result_command.command_type} command"
        )

    @staticmethod
    def __throw_invalid_deleted_document_with_deferred_command(result_command: CommandData) -> None:
        raise RuntimeError(
            f"Cannot perform save because document {result_command.key} has been deleted by "
            f"the session and is also taking part in deferred {result_command.command_type} command"
        )

    @staticmethod
    def __throw_no_database() -> None:
        raise RuntimeError(
            "Cannot open a Session without specyifing a name of a database to operate on. "
            "Database name can be passed as an argument when Session is being opened "
            "or default database can be defined using legacy.database property"
        )

    def _entity_changed(
        self, new_obj: dict, document_info: DocumentInfo, changes: Union[None, Dict[str, List[DocumentsChanges]]]
    ) -> bool:
        return JsonOperation.entity_changed(new_obj, document_info, changes)

    def has_changes(self) -> bool:
        for entity in self._documents_by_entity:
            entity: DocumentsByEntityHolder.DocumentsByEntityEnumeratorResult
            document = self.entity_to_json.convert_entity_to_json(entity.key, entity.value)
            if self._entity_changed(document, entity.value, None):
                return True

        return not len(self._deleted_entities) == 0

    def _what_changed(self) -> Dict[str, List[DocumentsChanges]]:
        changes = {}

        self.__get_all_entities_changes(changes)
        self.__prepare_for_entities_deletion(None, changes)

        return changes

    def __get_all_entities_changes(self, changes: Dict[str, List[DocumentsChanges]]) -> None:
        for key, value in self._documents_by_id.items():
            self.__update_metadata_modifications(value)
            new_obj = self.entity_to_json.convert_entity_to_json(value.entity, value)
            self._entity_changed(new_obj, value, changes)

    def ignore_changes_for(self, entity: object) -> None:
        self._get_document_info(entity).ignore_changes = True

    def _defer(self, *commands: CommandData) -> None:
        self._deferred_commands.extend(commands)
        for command in commands:
            self.__defer_internal(command)

    def __defer_internal(self, command: CommandData) -> None:
        if command.command_type is CommandType.BATCH_PATCH:
            command: BatchPatchCommandData
            for kvp in command.ids:
                self.__add_command(command, kvp.key, CommandType.PATCH, command.name)
            return
        self.__add_command(command, command.key, command.command_type, command.name)

    def __add_command(self, command: CommandData, key: str, command_type: CommandType, command_name: str) -> None:
        self._deferred_commands_map.update({IdTypeAndName.create(key, command_type, command_name): command})
        self._deferred_commands_map.update({IdTypeAndName.create(key, CommandType.CLIENT_ANY_COMMAND, None): command})

        if not any(
            [
                command.command_type == CommandType.ATTACHMENT_DELETE,
                command.command_type == CommandType.TIME_SERIES_COPY,
                command.command_type == CommandType.ATTACHMENT_MOVE,
                command.command_type == CommandType.ATTACHMENT_COPY,
                command.command_type == CommandType.ATTACHMENT_PUT,
                command.command_type == CommandType.TIME_SERIES,
                command.command_type == CommandType.COUNTERS,
            ]
        ):
            self._deferred_commands_map.update(
                {IdTypeAndName.create(key, CommandType.CLIENT_MODIFY_DOCUMENT_COMMAND, None): command}
            )

    def __close(self, is_disposing: bool) -> None:
        if self.__is_disposed:
            return

        self.session_closing_invoke(SessionClosingEventArgs(self))
        self.__is_disposed = True

    def close(self) -> None:
        self.__close(True)

    def register_missing(self, *keys: str) -> None:
        if self.no_tracking:
            return
        self._known_missing_ids.update(keys)

    def register_includes(self, includes: dict):
        if self.no_tracking:
            return

        if not includes:
            return

        for key, value in includes.items():
            if value is None:
                continue
            object_node = json.loads(json.dumps(value))
            new_document_info = DocumentInfo.get_new_document_info(object_node)
            if JsonExtensions.try_get_conflict(new_document_info.metadata):
                continue
            self._included_documents_by_id[new_document_info.key] = new_document_info

    def register_missing_includes(self, results, includes: dict, include_paths: List[str]):
        if self.no_tracking:
            return

        if not include_paths:
            return

        def __include(key):
            if not key:
                return
            if self.is_loaded(key):
                return

            document = includes.get(key)
            if document is not None:
                metadata = document.get(constants.Documents.Metadata.KEY)

                if JsonExtensions.try_get_conflict(metadata):
                    return

            self.register_missing()

        for result in results:
            for include in include_paths:
                if constants.Documents.Indexing.Fields.DOCUMENT_ID_FIELD_NAME == include:
                    continue
                IncludesUtil.include(result, include, __include)

    def register_counters(
        self,
        result_counters: dict,
        counters_to_include: Union[Dict[str, List[str]], List[str]],
        keys: List[str] = None,
        got_all: bool = None,
    ):
        keys_case = keys is not None and got_all is not None
        if self.no_tracking:
            return
        if not result_counters:
            if keys_case:
                if got_all:
                    for key in keys:
                        self.__set_got_all_counters_for_document(key)
                    return
            else:
                self.__set_got_all_in_cache_if_needed(counters_to_include)
        else:
            self._register_counters_internal(
                result_counters,
                {} if keys_case else counters_to_include,
                not keys_case,
                got_all if keys_case else False,
            )

        self.__register_missing_counters_for_keys(
            keys, counters_to_include
        ) if keys_case else self.__register_missing_counters(counters_to_include)

    def _register_counters_internal(
        self, result_counters: dict, counters_to_include: Dict[str, List[str]], from_query_result: bool, got_all: bool
    ):
        for key, result_counters in result_counters.items():
            if not result_counters:
                continue
            counters = []
            if from_query_result:
                counters = counters_to_include.get(key, None)
                got_all = True if counters is not None and len(counters) == 0 else False

            if len(result_counters) == 0 and not got_all:
                cache = self._counters_by_doc_id.get(key, None)
                if not cache:
                    continue
                for counter in counters:
                    del cache[1][counter]
                self._counters_by_doc_id[key] = cache
                continue
            self.__register_counters_for_document(key, got_all, result_counters, counters_to_include)

    def __register_counters_for_document(
        self, key: str, got_all: bool, result_counters: List[Dict], counters_to_include: Dict[str, List[str]]
    ):
        cache = self._counters_by_doc_id.get(key)
        if not cache:
            cache = [got_all, CaseInsensitiveDict()]

        deleted_counters = (
            set()
            if len(cache[1]) == 0
            else (set(cache[1].keys()) if len(counters_to_include.get(key)) == 0 else set(counters_to_include.get(key)))
        )

        for counter in result_counters:
            if counter:
                counter_name = counter["CounterName"]
                total_value = counter["TotalValue"]
            else:
                counter_name = total_value = None
            if counter_name and total_value:
                cache[1][counter_name] = total_value
                deleted_counters.discard(counter_name)

        if deleted_counters:
            for name in deleted_counters:
                del cache[1][name]

        cache[0] = got_all
        self._counters_by_doc_id[key] = cache

    def __set_got_all_in_cache_if_needed(self, counters_to_include: Dict[str, List[str]]):
        if not counters_to_include:
            return
        for key, value in counters_to_include:
            if len(value) > 0:
                continue
            self.__set_got_all_counters_for_document(key)

    def __set_got_all_counters_for_document(self, key: str):
        cache = self._counters_by_doc_id.get(key, None)
        if not cache:
            cache = [False, CaseInsensitiveDict()]
        cache[0] = True
        self._counters_by_doc_id[key] = cache

    def __register_missing_counters(self, counters_to_include: Dict[str, List[str]]):
        if not counters_to_include:
            return

        for key, value in counters_to_include.items():
            cache = self._counters_by_doc_id.get(key, None)
            if cache is None:
                cache = [False, CaseInsensitiveDict()]
                self._counters_by_doc_id[key] = cache

            for counter in value:
                if counter in cache[1]:
                    continue
                cache[1][counter] = None

    def __register_missing_counters_for_keys(self, keys: List[str], counters_to_include: List[str]):
        if not counters_to_include:
            return

        for counter in counters_to_include:
            for key in keys:
                cache = self._counters_by_doc_id.get(key)
                if not cache:
                    cache = [False, CaseInsensitiveDict()]
                    self._counters_by_doc_id[key] = cache

                if counter in cache[1]:
                    continue

                cache[1][counter] = None

    def register_time_series(self, result_time_series: dict):
        if self.no_tracking or not result_time_series:
            return

        for key, value in result_time_series:
            if not value:
                continue
            cache = self._time_series_by_doc_id.get(key, CaseInsensitiveDict())
            for inner_key, inner_value in value:
                if not inner_value:
                    continue
                name = inner_key
                for range_val in inner_value:
                    self.__add_to_cache(cache, range_val, name)

    @staticmethod
    def __add_to_cache(cache: Dict[str, List[TimeSeriesRangeResult]], new_range: TimeSeriesRangeResult, name: str):
        local_ranges = cache.get(name)
        if not local_ranges:
            cache[name] = list([new_range])
            return

        if local_ranges[0].from_date > new_range.to_date or local_ranges[-1].to_date < new_range.from_date:
            index = 0 if local_ranges[0].from_date > new_range.to_date else len(local_ranges)
            local_ranges[index] = new_range
            return

        to_range_index = int()
        from_range_index = -1
        range_already_in_cache = False

        for to_range_index in range(len(local_ranges)):
            if local_ranges[to_range_index].from_date <= new_range.from_date:
                if local_ranges[to_range_index].to_date >= new_range.to_date:
                    range_already_in_cache = True
                    break
                from_range_index = to_range_index
                continue
            if local_ranges[to_range_index].to_date >= new_range.to_date:
                break

        if range_already_in_cache:
            InMemoryDocumentSessionOperations.__update_existing_range(local_ranges[to_range_index], new_range)
            return

        merged_values = InMemoryDocumentSessionOperations.__merge_ranges(
            from_range_index, to_range_index, local_ranges, new_range
        )
        InMemoryDocumentSessionOperations.add_to_cache(
            name,
            new_range.from_date,
            new_range.to_date,
            from_range_index,
            to_range_index,
            local_ranges,
            cache,
            merged_values,
        )

    @staticmethod
    def add_to_cache(
        time_series: str,
        from_date: datetime.datetime,
        to_date: datetime.datetime,
        from_range_index: int,
        to_range_index: int,
        ranges: List[TimeSeriesRangeResult],
        cache: Dict[str, List[TimeSeriesRangeResult]],
        values: List[TimeSeriesEntry],
    ):
        if from_range_index == -1:
            # didn't find a 'from_range' => all ranges in cache start after 'from'

            if to_range_index == len(ranges):
                # the requested range [from, to] contains all the ranges that are in cache

                # e.g. if cache is : [[2,3], [4,5], [7,10]]
                # and the requested range is : [1,15]
                # after this action cache will be : [[1, 15]]

                time_series_range_result = TimeSeriesRangeResult(from_date, to_date, values)
                result = [time_series_range_result]
                cache[time_series] = result
                return

            if ranges[to_range_index].from_date > to_date:
                # requested range ends before 'toRange' starts
                # remove all ranges that come before 'toRange' from cache
                # add the new range at the beginning of the list

                # e.g. if cache is : [[2,3], [4,5], [7,10]]
                # and the requested range is : [1,6]
                # after this action cache will be : [[1,6], [7,10]]

                ranges[0:to_range_index].clear()
                time_series_range_result = TimeSeriesRangeResult(from_date, to_date, values)
                ranges.insert(0, time_series_range_result)
                return

            # the requested range ends inside 'toRange'
            # merge the result from server into 'toRange'
            # remove all ranges that come before 'toRange' from cache

            # e.g. if cache is : [[2,3], [4,5], [7,10]]
            # and the requested range is : [1,8]
            # after this action cache will be : [[1,10]]

            ranges[to_range_index].from_date = from_date
            ranges[to_range_index].entries = values
            ranges[0:to_range_index].clear()
            return

        # found a 'from_range'

        if to_range_index == len(ranges):
            # didn't find a 'to_range' => all the ranges in cache end before 'to'

            if ranges[from_range_index].to_date < from_date:
                # requested range starts after 'fromRange' ends,
                # so it needs to be placed right after it
                # remove all the ranges that come after 'fromRange' from cache
                # add the merged values as a new range at the end of the list

                # e.g. if cache is : [[2,3], [5,6], [7,10]]
                # and the requested range is : [4,12]
                # then 'fromRange' is : [2,3]
                # after this action cache will be : [[2,3], [4,12]]

                ranges[from_range_index + 1 : len(ranges)].clear()
                time_series_range_result = TimeSeriesRangeResult(from_date, to_date, values)
                ranges.append(time_series_range_result)
                return

            # the requested range starts inside 'fromRange'
            # merge result into 'fromRange'
            # remove all the ranges from cache that come after 'fromRange'

            # e.g. if cache is : [[2,3], [4,6], [7,10]]
            # and the requested range is : [5,12]
            # then 'fromRange' is [4,6]
            # after this action cache will be : [[2,3], [4,12]]

            ranges[from_range_index].to_date = to_date
            ranges[from_range_index].entries = values
            ranges[from_range_index + 1 : len(ranges)].clear()
            return

        # found both 'from_range' and 'to_range'
        # the requested range is inside cache bounds

        if ranges[from_range_index].to_date < from_date:
            # requested range starts after 'from_range' ends

            if ranges[to_range_index].from_date > to_date:
                # requested range ends before 'toRange' starts

                # remove all ranges in between 'fromRange' and 'toRange'
                # place new range in between 'fromRange' and 'toRange'

                # e.g. if cache is : [[2,3], [5,6], [7,8], [10,12]]
                # and the requested range is : [4,9]
                # then 'fromRange' is [2,3] and 'toRange' is [10,12]
                # after this action cache will be : [[2,3], [4,9], [10,12]]

                ranges[from_range_index + 1 : to_range_index].clear()
                time_series_range_result = TimeSeriesRangeResult(from_date, to_date, values)
                ranges.insert(from_range_index + 1, time_series_range_result)
                return

            # requested range ends inside 'toRange'

            # merge the new range into 'toRange'
            # remove all ranges in between 'fromRange' and 'toRange'

            # e.g. if cache is : [[2,3], [5,6], [7,10]]
            # and the requested range is : [4,9]
            # then 'fromRange' is [2,3] and 'toRange' is [7,10]
            # after this action cache will be : [[2,3], [4,10]]

            ranges[from_range_index + 1 : to_range_index].clear()
            ranges[to_range_index].from_date = from_date
            ranges[to_range_index].entries = values

            return

        # the requested range starts inside 'from_range'

        if ranges[to_range_index].from_date > to_date:
            # requested range ends before 'toRange' starts

            # remove all ranges in between 'fromRange' and 'toRange'
            # merge new range into 'fromRange'

            # e.g. if cache is : [[2,4], [5,6], [8,10]]
            # and the requested range is : [3,7]
            # then 'fromRange' is [2,4] and 'toRange' is [8,10]
            # after this action cache will be : [[2,7], [8,10]]

            ranges[from_range_index].to_date = to_date
            ranges[from_range_index].entries = values
            ranges[from_range_index + 1 : to_range_index].clear()

            return

        # the requested range starts inside 'fromRange'
        # and ends inside 'toRange'

        # merge all ranges in between 'fromRange' and 'toRange'
        # into a single range [fromRange.From, toRange.To]

        # e.g. if cache is : [[2,4], [5,6], [8,10]]
        # and the requested range is : [3,9]
        # then 'fromRange' is [2,4] and 'toRange' is [8,10]
        # after this action cache will be : [[2,10]]

        ranges[from_range_index].to_date = ranges[to_range_index].to_date
        ranges[from_range_index].entries = values
        ranges[from_range_index + 1 : to_range_index + 1].clear()

    @staticmethod
    def __merge_ranges(
        from_range_index: int,
        to_range_index: int,
        local_ranges: List[TimeSeriesRangeResult],
        new_range: TimeSeriesRangeResult,
    ) -> List[TimeSeriesEntry]:
        merged_values = []
        if from_range_index != -1 and local_ranges[from_range_index].to_date.time() >= new_range.from_date.time():
            for val in local_ranges[from_range_index].entries:
                if val.timestamp.time() >= new_range.from_date.time():
                    break
                merged_values.append(val)
        merged_values.extend(new_range.entries)

        if to_range_index < len(local_ranges) and local_ranges[to_range_index].from_date <= new_range.to_date:
            for val in local_ranges[to_range_index].entries:
                if val.timestamp.time() <= new_range.to_date.time():
                    continue
                merged_values.append(val)
        return merged_values

    @staticmethod
    def __update_existing_range(local_range: TimeSeriesRangeResult, new_range: TimeSeriesRangeResult) -> None:
        new_values = []
        for index in range(len(local_range.entries)):
            if local_range.entries[index].timestamp.time() >= new_range.from_date.time():
                break

            new_values.append(local_range.entries[index])

        new_values.extend(new_range.entries)

        for j in range(len(local_range.entries)):
            if local_range.entries[j].timestamp.time() <= new_range.to_date.time():
                continue
            new_values.append(local_range.entries[j])
        local_range.entries = new_values

    def hash_code(self) -> int:
        return self.__hash

    def __deserialize_from_transformer(
        self, object_type: Type[_T], key: Union[None, str], document: dict, track_entity: bool
    ) -> _T:
        return self.entity_to_json.convert_to_entity(object_type, key, document, track_entity)

    def check_if_id_already_included(self, ids: List[str], includes: Union[List[List], List[str]]) -> bool:
        if includes and not isinstance(includes[0], str):
            return self.check_if_id_already_included(ids, [arr[1] for arr in includes])
        includes: List[str]
        for key in ids:
            if key in self._known_missing_ids:
                continue

            document_info = self._documents_by_id.get(key)
            if document_info is None:
                document_info = self._included_documents_by_id.get(key)
                if document_info is None:
                    return False

            if document_info.entity is None and document_info.document is None:
                return False

            if includes is None:
                continue

            for include in includes:
                has_all = [True]
                # todo: make sure it'll work - we create has_all and praise for changes on the reference has_all[0]
                IncludesUtil.include(document_info.document, include, lambda s: has_all[0] and self.is_loaded(s))
                if not has_all[0]:
                    return False
        return True

    def _refresh_internal(self, entity: object, cmd: RavenCommand, document_info: DocumentInfo):
        if not isinstance(cmd.result, GetDocumentsResult):
            raise TypeError(f"Unexpected RavenCommand.result type '{type(cmd.result)}' - expected 'GetDocumentResult'")
        try:
            document = cmd.result.results[0]
        except IndexError:
            raise ValueError(f"Document {document_info.key} no longer exists and was probably deleted")

        value = document.get(constants.Documents.Metadata.KEY)
        document_info.metadata = value

        if document_info.metadata is not None:
            change_vector = value.get(constants.Documents.Metadata.CHANGE_VECTOR)
            document_info.change_vector = change_vector

        if document_info.entity is not None and not self.no_tracking:
            self.entity_to_json.remove_from_missing(document_info.entity)

        document_info.entity = self.entity_to_json.convert_to_entity(
            type(entity), document_info.key, document, not self.no_tracking
        )
        document_info.document = document

        try:
            entity = deepcopy(document_info.entity)
        except Error as e:
            raise RuntimeError(f"Unable to refresh entity: {e.args[0]}", e)

        document_info_by_id = self._documents_by_id.get(document_info.key)
        if document_info_by_id is not None:
            document_info_by_id.entity = entity
        return entity

    def _get_operation_result(self, object_type: Type[_T], result: _T) -> _T:
        if result is None:
            return Utils.get_default_value(object_type)

        if issubclass(type(result), object_type) or type(result) == object_type:
            return result
            # todo: cast result on object_type
        if isinstance(result, dict):
            if not result:
                return None
            return result.values().__iter__().__next__()
            # todo: cast result on object_type
        raise TypeError(f"Unable to cast {result.__class__.__name__} to {object_type.__name__}")

    # todo: implement method below
    def update_session_after_save_changes(self, result: BatchCommandResult):
        returned_transaction_index = result.transaction_index

    def _process_query_parameters(
        self, object_type: type, index_name: str, collection_name: str, conventions: DocumentConventions
    ) -> (str, str):
        is_index = index_name and not index_name.isspace()
        is_collection = collection_name and not collection_name.isspace()

        if is_index and is_collection:
            raise ValueError(
                "Parameters index_name and collection_name are mutually exclusive. Please specify only one."
            )

        if not is_collection and not is_index:
            collection_name = conventions.get_collection_name(object_type)
            collection_name = (
                collection_name if collection_name else constants.Documents.Metadata.ALL_DOCUMENTS_COLLECTION
            )

        return index_name, collection_name

    class ReplicationWaitOptsBuilder:
        def __init__(self, session: InMemoryDocumentSessionOperations):
            self.__session = session

        def get_options(self) -> BatchOptions:
            if self.__session._save_changes_options is None:
                self.__session._save_changes_options = BatchOptions()

            if self.__session._save_changes_options.replication_options is None:
                self.__session._save_changes_options.replication_options = ReplicationBatchOptions()

            return self.__session._save_changes_options

        def with_timeout(
            self, timeout: datetime.timedelta
        ) -> InMemoryDocumentSessionOperations.ReplicationWaitOptsBuilder:
            self.get_options().replication_options.wait_for_indexes_timeout = timeout
            return self

        def throw_on_timeout(self, should_throw: bool) -> InMemoryDocumentSessionOperations.ReplicationWaitOptsBuilder:
            self.get_options().replication_options.throw_on_timeout_in_wait_for_replicas = should_throw
            return self

        def number_of_replicas(self, replicas: int) -> InMemoryDocumentSessionOperations.ReplicationWaitOptsBuilder:
            self.get_options().replication_options.number_of_replicas_to_wait_for = replicas
            return self

        def majority(self, wait_for_majority: bool) -> InMemoryDocumentSessionOperations.ReplicationWaitOptsBuilder:
            self.get_options().replication_options.majority = wait_for_majority
            return self

    class IndexesWaitOptsBuilder:
        def __init__(self, session: InMemoryDocumentSessionOperations):
            self.__session = session

        def get_options(self) -> BatchOptions:
            if self.__session._save_changes_options is None:
                self.__session._save_changes_options = BatchOptions()

            if self.__session._save_changes_options.index_options is None:
                self.__session._save_changes_options.index_options = IndexBatchOptions()

            return self.__session._save_changes_options

        def with_timeout(self, timeout: datetime.timedelta) -> InMemoryDocumentSessionOperations.IndexesWaitOptsBuilder:
            self.get_options().index_options.wait_for_indexes_timeout = timeout
            return self

        def throw_on_timeout(self, should_throw: bool) -> InMemoryDocumentSessionOperations.IndexesWaitOptsBuilder:
            self.get_options().index_options.throw_on_timeout_in_wait_for_replicas = should_throw
            return self

        def wait_for_indexes(self, *indexes: str) -> InMemoryDocumentSessionOperations.IndexesWaitOptsBuilder:
            self.get_options().index_options.wait_for_indexes = indexes
            return self

    class SaveChangesData:
        def __init__(self, session: InMemoryDocumentSessionOperations):
            self.deferred_commands: List[CommandData] = session._deferred_commands
            self.deferred_commands_map: Dict[IdTypeAndName, CommandData] = session._deferred_commands_map
            self.session_commands: List[CommandData] = []
            self.entities: List = []
            self.options = session._save_changes_options
            self.on_success = InMemoryDocumentSessionOperations.SaveChangesData.ActionsToRunOnSuccess(session)

        class ActionsToRunOnSuccess:
            def __init__(self, session: InMemoryDocumentSessionOperations):
                self.__session = session
                self.__documents_by_id_to_remove: List[str] = []
                self.__documents_by_entity_to_remove: List = []
                self.__document_infos_to_update: List[Tuple[DocumentInfo, dict]] = []
                self.__clear_deleted_entities: bool = False

            def remove_document_by_id(self, key: str):
                self.__documents_by_id_to_remove.append(key)

            def remove_document_by_entity(self, entity):
                self.__documents_by_entity_to_remove.append(entity)

            def update_entity_document_info(self, document_info: DocumentInfo, document: dict):
                self.__document_infos_to_update.append((document_info, document))

            def clear_session_state_after_successful_save_changes(self):
                for key in self.__documents_by_id_to_remove:
                    self.__session._documents_by_id.pop(key)
                for key in self.__documents_by_entity_to_remove:
                    self.__session._documents_by_entity.pop(key)

                for document_info_dict_tuple in self.__document_infos_to_update:
                    info: DocumentInfo = document_info_dict_tuple[0]
                    document: dict = document_info_dict_tuple[1]
                    info.new_document = False
                    info.document = document

                if self.__clear_deleted_entities:
                    self.__session._deleted_entities.clear()

                self.__session._deferred_commands.clear()
                self.__session._deferred_commands_map.clear()

            def clear_deleted_entities(self) -> None:
                self.__clear_deleted_entities = True
