import datetime
import enum
import logging
from typing import Union, Optional, TypeVar, List, Type, Callable, Dict, TYPE_CHECKING

from ravendb import constants
from ravendb.documents.commands.query import QueryCommand
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.query import QueryResult
from ravendb.documents.session.event_args import BeforeConversionToEntityEventArgs, AfterConversionToEntityEventArgs
from ravendb.documents.session.tokens.query_tokens.definitions import FieldsToFetchToken
from ravendb.exceptions.documents.indexes import IndexDoesNotExistException
from ravendb.tools.utils import Stopwatch, Utils

if TYPE_CHECKING:
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class QueryOperation:
    __logger = logging.getLogger("QueryOperation")
    _T = TypeVar("_T")

    # __facet_result_fields =
    def __init__(
        self,
        session: "InMemoryDocumentSessionOperations",
        index_name: str,
        index_query: IndexQuery,
        fields_to_fetch: FieldsToFetchToken,
        disable_entities_tracking: bool,
        metadata_only: bool,
        index_entries_only: bool,
        is_project_into: bool,
    ):
        self.__session = session
        self.__index_name = index_name
        self.__index_query = index_query
        self.__fields_to_fetch = fields_to_fetch
        self.__no_tracking = disable_entities_tracking
        self.__metadata_only = metadata_only
        self.__index_entries_only = index_entries_only
        self.__is_project_into = is_project_into

        self.__current_query_results: Union[None, QueryResult] = None
        self.__sp: Union[None, Stopwatch] = Stopwatch()
        self.__assert_page_size_set()

    def create_request(self) -> QueryCommand:
        self.__session.increment_requests_count()
        self.log_query()
        return QueryCommand(self.__session, self.__index_query, self.__metadata_only, self.__index_entries_only)

    @property
    def current_query_results(self) -> QueryResult:
        return self.__current_query_results

    def set_result(self, query_result: QueryResult):
        self.ensure_is_acceptable_and_save_result(query_result)

    def __assert_page_size_set(self) -> None:
        if not self.__session.conventions.throw_if_query_page_size_is_not_set:
            return

        if self.__index_query.is_page_size_set:
            return

        raise RuntimeError(
            "Attempt to query without explicitly specifying a page size. "
            "You can use .take() methods to set maximum number of results. By default the page size is "
            "set to 0x7fffff and can cause severe performance degradation."
        )

    def __start_timing(self) -> None:
        self.__sp = Stopwatch().start()

    def log_query(self) -> None:
        self.__logger.info(
            f"Executing query {self.__index_query.query} "
            f"on index {self.__index_name} "
            f"in {self.__session.advanced.store_identifier}"
        )

    def enter_query_context(self) -> None:  # todo: make it return Closeable
        self.__start_timing()

    # def complete_as_array(self, object_type: Type[_T]) -> List[_T]:
    #    query_result = self.__current_query_results.create_snapshot()
    #    result: List[_T] = [None]
    #    self.complete_internal(object_type, query_result, lambda idx, item: resource)

    def complete(self, object_type: Type[_T]) -> List[_T]:
        query_result = self.__current_query_results.create_snapshot()
        result = list()
        self.__complete_internal(object_type, query_result, result.insert)
        return result

    def __complete_internal(
        self,
        object_type: Type[_T],
        query_result: QueryResult[List[Dict[str, Dict[str, str]]], None],
        add_to_result: Callable[[int, _T], None],
    ) -> None:
        if not self.__no_tracking:
            self.__session.register_includes(query_result.includes)

        for i in range(len(query_result.results)):
            document = query_result.results[i]
            metadata = document.get(constants.Documents.Metadata.KEY)
            id_node = metadata.get(constants.Documents.Metadata.ID)
            key = None
            if isinstance(id_node, str):
                key = id_node

            add_to_result(
                i,
                self.deserialize(
                    object_type,
                    key,
                    document,
                    metadata,
                    self.__fields_to_fetch,
                    self.__no_tracking,
                    self.__session,
                    self.__is_project_into,
                ),
            )
        if not self.__no_tracking:
            self.__session.register_missing_includes(
                query_result.results, query_result.includes, query_result.included_paths
            )

            # todo: counters and timeseries
            if query_result.counter_includes is not None:
                raise NotImplementedError()

            if query_result.time_series_includes is not None:
                raise NotImplementedError()

            if query_result.compare_exchange_value_includes is not None:
                self.__session.cluster_transaction.register_compare_exchange_values(
                    query_result.compare_exchange_value_includes
                )

    @staticmethod
    def deserialize(
        object_type: Type[_T],
        key: str,
        document: dict,
        metadata: dict,
        fields_to_fetch: FieldsToFetchToken,
        disable_entities_tracking: bool,
        session: "InMemoryDocumentSessionOperations",
        is_project_into: bool,
    ):
        projection = metadata.get("@projection")
        if not projection:
            return session.track_entity(object_type, key, document, metadata, disable_entities_tracking)

        if (
            fields_to_fetch is not None
            and fields_to_fetch.projections is not None
            and len(fields_to_fetch.projections) == 1
        ):
            projection_field = fields_to_fetch.projections[0]

            if fields_to_fetch.source_alias is not None:
                if projection_field.startswith(fields_to_fetch.source_alias):
                    projection_field = projection_field[len(fields_to_fetch.source_alias) + 1 :]

                if projection_field.startswith("'"):
                    projection_field = projection_field[1:-1]

            if object_type == str or object_type in Utils.primitives_and_collections or object_type == enum.Enum:
                json_node = document.get(projection_field)
                if isinstance(json_node, object_type):
                    return json_node
                raise TypeError(f'The value under projection field "{projection_field}" isn\'t a "{object_type}"')

            # todo: time series
            # is_time_series_field = fields_to_fetch.projections[0].startswith(constants.TimeSeries.QUERY_FUNCTION)

            if not is_project_into:  # or is_time_series_field
                inner = document.get(projection_field)
                if inner is None:
                    return Utils.get_default_value(object_type)

                if (
                    fields_to_fetch.fields_to_fetch is not None
                    and fields_to_fetch.fields_to_fetch[0] == fields_to_fetch.projections[0]
                ):
                    document = inner

        if object_type == dict:
            return document

        session.before_conversion_to_entity_invoke(
            BeforeConversionToEntityEventArgs(session, key, object_type, document)
        )

        result = Utils.initialize_object(document, object_type)
        session.after_conversion_to_entity_invoke(AfterConversionToEntityEventArgs(session, key, document, result))

        return result

    @property
    def no_tracking(self) -> bool:
        return self.__no_tracking

    @no_tracking.setter
    def no_tracking(self, value: bool):
        self.__no_tracking = value

    def ensure_is_acceptable_and_save_result(
        self, result: QueryResult, duration: Optional[datetime.timedelta] = None
    ) -> None:
        if duration is not None:
            if result is None:
                raise IndexDoesNotExistException(f"Could not find index {self.__index_name}")
            self.ensure_is_acceptable(result, self.__index_query.wait_for_non_stale_results, duration, self.__session)

            self.__save_query_result(result)

        elif self.__sp is None:
            self.ensure_is_acceptable_and_save_result(result, None)
        else:
            self.__sp.stop()
            self.ensure_is_acceptable_and_save_result(result, self.__sp.elapsed())

    def __save_query_result(self, result: QueryResult) -> None:
        self.__current_query_results = result

        is_stale = " stale " if result.is_stale else " "
        parameters = []
        if self.__index_query.query_parameters:
            parameters.append("(parameters: ")

            first = True

            for key, value in self.__index_query.query_parameters.items():
                if not first:
                    parameters.append(", ")

                parameters.append(key)
                parameters.append(" = ")
                parameters.append(value)

                first = False

            parameters.append(") ")

        self.__logger.info(
            f"Query {self.__index_query.query} {''.join(list(map(str,parameters)))}returned "
            f"{len(result.results)}{is_stale}results (total index results: {result.total_results})"
        )

    @staticmethod
    def ensure_is_acceptable(
        result: QueryResult,
        wait_for_non_stale_results: bool,
        duration: Union[datetime.timedelta, Stopwatch],
        session: "InMemoryDocumentSessionOperations",
    ):
        if isinstance(duration, datetime.timedelta):
            if wait_for_non_stale_results and result.is_stale:
                elapsed = "" if duration is None else f" {(duration.microseconds / 1000)} ms"
                msg = f"Waited {elapsed} for the query to return non stale result."
                raise TimeoutError(msg)

    @property
    def index_query(self) -> IndexQuery:
        return self.__index_query
