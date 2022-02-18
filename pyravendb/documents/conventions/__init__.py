from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyravendb.documents.session.in_memory_document_session_operations


class ShouldIgnoreEntityChanges:
    @abstractmethod
    def check(
        self,
        session_operations: "pyravendb.documents.session.in_memory_document_session_operations.InMemoryDocumentSessionOperations",
        entity: object,
        document_id: str,
    ) -> bool:
        pass
