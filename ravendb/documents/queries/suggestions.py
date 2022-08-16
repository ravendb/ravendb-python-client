from __future__ import annotations

import enum
from abc import abstractmethod, ABC
from typing import List, Generic, TypeVar, Dict, Union, Optional, Callable, TYPE_CHECKING

from ravendb.documents.commands.query import QueryCommand
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.query import QueryResult
from ravendb.documents.session.operations.lazy import LazySuggestionQueryOperation
from ravendb.documents.session.operations.query import QueryOperation
from ravendb.tools.utils import Stopwatch

_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.store import Lazy
    from ravendb.documents.session.query import DocumentQuery
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb.documents.session import InMemoryDocumentSessionOperations


class StringDistanceTypes(enum.Enum):
    NONE = "None"
    DEFAULT = "Default"
    LEVENSHTEIN = "Levenshtein"
    JARO_WINKLER = "JaroWinkler"
    N_GRAM = "NGram"


class SuggestionSortMode(enum.Enum):
    NONE = "None"
    POPULARITY = "Popularity"


class SuggestionBase:
    def __init__(self, field: str = None, display_field: str = None, options: SuggestionOptions = None):
        self.field = field
        self.display_field = display_field
        self.options = options


class SuggestionOptions:
    @classmethod
    def default_options(cls) -> SuggestionOptions:
        return cls()

    DEFAULT_ACCURACY = 0.5
    DEFAULT_PAGE_SIZE = 15
    DEFAULT_DISTANCE = StringDistanceTypes.LEVENSHTEIN
    DEFAULT_SORT_MODE = SuggestionSortMode.POPULARITY

    def __init__(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        distance: StringDistanceTypes = DEFAULT_DISTANCE,
        accuracy: float = DEFAULT_ACCURACY,
        sort_mode: SuggestionSortMode = DEFAULT_SORT_MODE,
    ):
        self.page_size = page_size
        self.distance = distance
        self.accuracy = accuracy
        self.sort_mode = sort_mode

    def to_json(self) -> dict:
        return {
            "pageSize": self.page_size,
            "distance": self.distance,
            "accuracy": self.accuracy,
            "sortMode": self.sort_mode,
        }


class SuggestionWithTerm(SuggestionBase):
    def __init__(self, field: str, term: str = None):
        super().__init__(field)
        self.term = term


class SuggestionWithTerms(SuggestionBase):
    def __init__(self, field: str, terms: List[str] = None):
        super().__init__(field)
        self.terms = terms


class SuggestionResult:
    def __init__(self, name: str = None, suggestions: List[str] = None):
        self.name = name
        self.suggestions = suggestions

    @classmethod
    def from_json(cls, json_dict: dict) -> SuggestionResult:
        return cls(json_dict["Name"], json_dict["Suggestions"])


class SuggestionOperations(Generic[_T]):
    @abstractmethod
    def with_display_name(self, display_name: str) -> SuggestionOperations[_T]:
        pass

    @abstractmethod
    def with_options(self, options: SuggestionOptions) -> SuggestionOperations[_T]:
        pass


class SuggestionBuilder(Generic[_T], SuggestionOperations[_T]):
    def __init__(self):
        self.__term: Union[None, SuggestionWithTerm] = None
        self.__terms: Union[None, SuggestionWithTerms] = None

    @property
    def suggestion(self) -> SuggestionBase:
        if self.__term is not None:
            return self.__term

        return self.__terms

    def with_display_name(self, display_name: str) -> SuggestionOperations[_T]:
        self.suggestion.display_field = display_name
        return self

    def by_field(self, field_name: str, term_or_terms: Union[str, List[str]]) -> SuggestionOperations[_T]:
        if field_name is None:
            raise ValueError("field_name cannot be None")

        if term_or_terms is None:
            raise ValueError("term cannot be None")

        if isinstance(term_or_terms, str):
            self.__term = SuggestionWithTerm(field_name)
            self.__term.term = term_or_terms

        elif isinstance(term_or_terms, list):
            if not term_or_terms:
                raise ValueError("terms cannot be an empty collection")

            self.__terms = SuggestionWithTerms(field_name)
            self.__terms.terms = term_or_terms

        return self

    def with_options(self, options: SuggestionOptions) -> SuggestionOperations[_T]:
        self.suggestion.options = options
        return self


class SuggestionQueryBase(ABC):
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.__session = session

        self.__duration: Union[None, Stopwatch] = None
        self.__query: Union[None, IndexQuery] = None

    def __str__(self):
        return str(self._get_index_query(False))

    def execute(self) -> Dict[str, SuggestionResult]:
        command = self.__command

        self.__duration = Stopwatch.create_started()
        self.__session.increment_requests_count()
        self.__session.advanced.request_executor.execute_command(command)

        return self.__process_results(command.result)

    def __process_results(self, query_result: QueryResult) -> Dict[str, SuggestionResult]:
        self._invoke_after_query_executed(query_result)

        try:
            results = {}
            for result in query_result.results:
                suggestion_result = SuggestionResult.from_json(result)
                results[suggestion_result.name] = suggestion_result

            QueryOperation.ensure_is_acceptable(
                query_result, self.__query.wait_for_non_stale_results, self.__duration, self.__session
            )

            return results
        except Exception as e:
            raise RuntimeError(f"Unable to process suggestion results: ", e)

    def execute_lazy(
        self, on_eval: Optional[Callable[[Dict[str, SuggestionResult]], None]] = None
    ) -> "Lazy[Dict[str, SuggestionResult]]":
        self.__query = self._get_index_query()
        self.__session: "DocumentSession"
        return self.__session.add_lazy_operation(
            dict,
            LazySuggestionQueryOperation(
                self.__session, self.__query, self._invoke_after_query_executed, self.__process_results
            ),
            on_eval,
        )

    @abstractmethod
    def _get_index_query(self, update_after_query_executed: Optional[bool] = None) -> IndexQuery:
        pass

    @abstractmethod
    def _invoke_after_query_executed(self, result: QueryResult) -> None:
        pass

    @property
    def __command(self) -> QueryCommand:
        self.__query = self._get_index_query()
        return QueryCommand(self.__session, self.__query, False, False)


class SuggestionDocumentQuery(Generic[_T], SuggestionQueryBase):
    def __init__(self, source: "DocumentQuery[_T]"):
        super().__init__(source.session)
        self.__source = source

    def _get_index_query(self, update_after_query_executed: Optional[bool] = True) -> IndexQuery:
        return self.__source.index_query

    def _invoke_after_query_executed(self, result: QueryResult) -> None:
        self.__source.invoke_after_query_executed(result)

    def and_suggest_using(
        self, suggestion_or_builder: Union[SuggestionBase, Callable[[SuggestionBuilder[_T]], None]]
    ) -> SuggestionDocumentQuery[_T]:
        if not isinstance(suggestion_or_builder, SuggestionBase):
            f = SuggestionBuilder()
            suggestion_or_builder(f)
            suggestion_or_builder = f.suggestion
        self.__source.suggest_using(suggestion_or_builder)
        return self
