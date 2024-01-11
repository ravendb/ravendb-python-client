import datetime
from typing import TYPE_CHECKING, TypeVar, Type, List, Dict

from ravendb.documents.operations.lazy.revisions import LazyRevisionOperations
from ravendb.documents.session.misc import ForceRevisionStrategy
from ravendb.documents.session.operations.operations import GetRevisionOperation, GetRevisionsCountOperation

if TYPE_CHECKING:
    from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
        InMemoryDocumentSessionOperations,
    )
    from ravendb.documents.commands.batches import CommandData
    from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
    from ravendb.documents.session.document_session import DocumentSession


_T = TypeVar("_T")


class AdvancedSessionExtensionBase:
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.session = session
        self.request_executor = session.request_executor
        self.session_info = session.session_info
        self.document_store = session.document_store
        self.deferred_commands_map = session.deferred_commands_map
        self.documents_by_id = session.documents_by_id

    def defer(self, *commands: "CommandData") -> None:
        self.session.defer(*commands)


class DocumentSessionRevisionsBase(AdvancedSessionExtensionBase):
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        super().__init__(session)

    def force_revision_creation_for(
        self, entity: _T, strategy: ForceRevisionStrategy = ForceRevisionStrategy.BEFORE
    ) -> None:
        if entity is None:
            raise ValueError("Entity cannot be None")

        document_info = self.session.documents_by_entity.get(entity, None)
        if document_info is None:
            raise RuntimeError(
                "Cannot create a revision for the requested entity because it is not tracked by the session"
            )

        self._add_id_to_list(document_info.key, strategy)

    def force_revision_creation_for_id(
        self, id: str, strategy: ForceRevisionStrategy = ForceRevisionStrategy.BEFORE
    ) -> None:
        self._add_id_to_list(id, strategy)

    def _add_id_to_list(self, id_: str, requested_strategy: ForceRevisionStrategy) -> None:
        if not id_:
            raise ValueError("Id cannot be None or empty")

        existing_strategy = self.session.ids_for_creating_forced_revisions.get(id_)
        id_already_added = existing_strategy is not None

        if id_already_added and existing_strategy != requested_strategy:
            raise RuntimeError(
                f"A request for creating a revision was already made for document {id_} in the current session "
                f"but with a different force strategy. "
                f"New strategy requested: {requested_strategy}. "
                f"Previous strategy: {existing_strategy}."
            )

        if not id_already_added:
            self.session.ids_for_creating_forced_revisions[id_] = requested_strategy


class DocumentSessionRevisions(DocumentSessionRevisionsBase):
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        super().__init__(session)

    @property
    def lazily(self) -> LazyRevisionOperations:
        self.session: "DocumentSession"
        return LazyRevisionOperations(self.session)

    def get_for(self, id_: str, object_type: Type[_T] = None, start: int = 0, page_size: int = 25) -> List[_T]:
        operation = GetRevisionOperation.from_start_page(self.session, id_, start, page_size)
        command = operation.create_request()

        if command is None:
            return operation.get_revisions_for(object_type)

        if self.session_info is not None:
            self.session_info.increment_request_count()

        self.request_executor.execute_command(command, self.session_info)
        operation.set_result(command.result)
        return operation.get_revisions_for(object_type)

    def get_metadata_for(self, id_: str, start: int = 0, page_size: int = 25) -> List["MetadataAsDictionary"]:
        operation = GetRevisionOperation.from_start_page(self.session, id_, start, page_size, True)
        command = operation.create_request()
        if command is None:
            return operation.get_revisions_metadata_for()

        if self.session_info is not None:
            self.session_info.increment_request_count()

        self.request_executor.execute_command(command, self.session_info)
        operation.set_result(command.result)
        return operation.get_revisions_metadata_for()

    def get_by_change_vector(self, change_vector: str, object_type: Type[_T] = None) -> _T:
        operation = GetRevisionOperation.from_change_vector(self.session, change_vector)
        command = operation.create_request()
        if command is None:
            return operation.get_revision(object_type)

        if self.session_info is not None:
            self.session_info.increment_request_count()

        self.request_executor.execute_command(command, self.session_info)
        operation.set_result(command.result)
        return operation.get_revision(object_type)

    def get_by_change_vectors(self, change_vectors: List[str], object_type: Type[_T] = None) -> Dict[str, _T]:
        operation = GetRevisionOperation.from_change_vectors(self.session, change_vectors)
        command = operation.create_request()
        if command is None:
            return operation.get_revisions(object_type)

        if self.session_info is not None:
            self.session_info.increment_request_count()

        self.request_executor.execute_command(command, self.session_info)
        operation.set_result(command.result)
        return operation.get_revisions(object_type)

    def get_by_before_date(self, id_: str, before_date: datetime.datetime, object_type: Type[_T] = None) -> _T:
        operation = GetRevisionOperation.from_before_date(self.session, id_, before_date)
        command = operation.create_request()
        if command is None:
            return operation.get_revision(object_type)

        if self.session_info is not None:
            self.session_info.increment_request_count()

        self.request_executor.execute_command(command, self.session_info)
        operation.set_result(command.result)
        return operation.get_revision(object_type)

    def get_count_for(self, id_: str) -> int:
        operation = GetRevisionsCountOperation(id_)
        command = operation.create_request()
        if self.session_info is not None:
            self.session_info.increment_request_count()
        self.request_executor.execute_command(command, self.session_info)
        return command.result
