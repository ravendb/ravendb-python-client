from abc import abstractmethod

from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class ShouldIgnoreEntityChanges:
    @abstractmethod
    def check(self, session_operations: InMemoryDocumentSessionOperations, entity: object, document_id: str) -> bool:
        pass
