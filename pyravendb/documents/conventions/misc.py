from abc import abstractmethod
from typing import TYPE_CHECKING
from typing import TypeVar, Generic, Tuple

if TYPE_CHECKING:
    from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations

_T = TypeVar("_T")


class ValueForQueryConverter(Generic[_T]):
    @abstractmethod
    def try_to_convert_value_for_query(self, field_name: str, value: _T, for_range: bool) -> Tuple[bool, object]:
        pass


class ShouldIgnoreEntityChanges:
    @abstractmethod
    def check(
        self,
        session_operations: "InMemoryDocumentSessionOperations",
        entity: object,
        document_id: str,
    ) -> bool:
        pass
