from abc import abstractmethod
from typing import TypeVar, Union, Dict, Callable, Optional, Generic, TYPE_CHECKING

from ravendb.documents.commands.query import QueryCommand
from ravendb.documents.queries.facets.builders import FacetBuilder
from ravendb.documents.queries.facets.definitions import FacetBase
from ravendb.documents.queries.facets.misc import FacetResult
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.query import QueryResult
from ravendb.documents.session.operations.lazy import LazyAggregationQueryOperation
from ravendb.documents.session.operations.query import QueryOperation
from ravendb.documents.store.lazy import Lazy
from ravendb.tools.utils import Stopwatch

_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.session import InMemoryDocumentSessionOperations
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb.documents.session.query import DocumentQuery, RawDocumentQuery


class AggregationQueryBase:
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.__session = session
        self.__query: Union[None, IndexQuery] = None
        self.__duration: Union[None, Stopwatch] = None

    def execute(self) -> Dict[str, FacetResult]:
        command = self.__command
        self.__duration = Stopwatch.create_started()

        self.__session.increment_requests_count()
        self.__session.advanced.request_executor.execute_command(command)

        return self.__process_results(command.result)

    def execute_lazy(
        self, on_eval: Optional[Callable[[Dict[str, FacetResult]], None]] = None
    ) -> Lazy[Dict[str, FacetResult]]:
        self.__query = self._get_index_query()
        self.__session: "DocumentSession"
        return self.__session.add_lazy_operation(
            dict,
            LazyAggregationQueryOperation(
                self.__session,
                self.__query,
                lambda result: self._invoke_after_query_executed(result),
                self.__process_results,
            ),
            on_eval,
        )

    @property
    def __command(self) -> QueryCommand:
        self.__query = self._get_index_query()
        return QueryCommand(self.__session, self.__query, False, False)

    @abstractmethod
    def _get_index_query(self, update_after_query_executed: Optional[bool] = None) -> IndexQuery:
        pass

    @abstractmethod
    def _invoke_after_query_executed(self, result: QueryResult) -> None:
        pass

    def __process_results(self, query_results: QueryResult) -> Dict[str, FacetResult]:
        self._invoke_after_query_executed(query_results)

        results: Dict[str, FacetResult] = {}

        for result in query_results.results:
            facet_result = FacetResult.from_json(result)
            results[facet_result.name] = facet_result

        self.__session.register_includes(query_results.includes)

        QueryOperation.ensure_is_acceptable(
            query_results, self.__query.wait_for_non_stale_results, self.__duration, self.__session
        )
        return results

    def to_string(self):
        return str(self._get_index_query(False))


class AggregationDocumentQuery(Generic[_T], AggregationQueryBase):
    def __init__(self, source: "DocumentQuery[_T]"):
        super().__init__(source.session)
        self.__source = source

    def and_aggregate_by(self, builder_or_facet: Union[Callable[[FacetBuilder[_T]], None], FacetBase]):
        if isinstance(builder_or_facet, FacetBase):
            facet = builder_or_facet
        else:
            f = FacetBuilder()
            builder_or_facet(f)
            facet = f.facet

        self.__source.aggregate_by(facet)
        return self

    def _get_index_query(self, update_after_query_executed: Optional[bool] = True) -> IndexQuery:
        return self.__source.index_query

    def _invoke_after_query_executed(self, result: QueryResult) -> None:
        self.__source.invoke_after_query_executed(result)


class AggregationRawDocumentQuery(Generic[_T], AggregationQueryBase):
    def __init__(self, source: "RawDocumentQuery[_T]", session: "InMemoryDocumentSessionOperations"):
        super().__init__(session)
        self.__source = source

        if source is None:
            raise ValueError("Source cannot be None")

    def _get_index_query(self, update_after_query_executed: Optional[bool] = True) -> IndexQuery:
        return self.__source.index_query

    def _invoke_after_query_executed(self, result: QueryResult) -> None:
        self.__source.invoke_after_query_executed(result)
