from __future__ import annotations
import datetime
import enum
import os
from copy import copy
from typing import (
    Generic,
    TypeVar,
    List,
    Optional,
    Callable,
    Dict,
    Union,
    Set,
    Tuple,
    Collection,
    Type,
    Iterator,
    TYPE_CHECKING,
)

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.indexes.spatial.configuration import SpatialUnits, SpatialRelation
from ravendb.documents.queries.explanation import Explanations, ExplanationOptions
from ravendb.documents.queries.facets.builders import FacetBuilder
from ravendb.documents.queries.facets.definitions import FacetBase
from ravendb.documents.queries.facets.queries import AggregationDocumentQuery, AggregationRawDocumentQuery
from ravendb.documents.queries.group_by import GroupBy
from ravendb.documents.queries.highlighting import QueryHighlightings, HighlightingOptions, Highlightings
from ravendb.documents.queries.index_query import IndexQuery, Parameters
from ravendb.documents.queries.misc import SearchOperator
from ravendb.documents.queries.more_like_this import (
    MoreLikeThisScope,
    MoreLikeThisBase,
    MoreLikeThisBuilder,
    MoreLikeThisUsingDocument,
    MoreLikeThisUsingDocumentForDocumentQuery,
)
from ravendb.documents.queries.query import QueryOperator, QueryResult, QueryTimings, ProjectionBehavior, QueryData
from ravendb.documents.queries.spatial import DynamicSpatialField, SpatialCriteria, SpatialCriteriaFactory
from ravendb.documents.queries.suggestions import (
    SuggestionBase,
    SuggestionWithTerm,
    SuggestionOptions,
    SuggestionWithTerms,
    SuggestionDocumentQuery,
    SuggestionBuilder,
)
from ravendb.documents.queries.utils import QueryFieldUtil
from ravendb.documents.session.event_args import BeforeQueryEventArgs
from ravendb.documents.session.loaders.include import IncludeBuilderBase, QueryIncludeBuilder
from ravendb.documents.session.misc import MethodCall, CmpXchg, OrderingType, DocumentQueryCustomization
from ravendb.documents.session.operations.lazy import LazyQueryOperation
from ravendb.documents.session.operations.query import QueryOperation
from ravendb.documents.session.query_group_by import GroupByDocumentQuery
from ravendb.documents.session.tokens.misc import WhereOperator
from ravendb.documents.session.tokens.query_tokens.facets import FacetToken
from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
from ravendb.documents.session.tokens.query_tokens.definitions import (
    DeclareToken,
    LoadToken,
    FromToken,
    FieldsToFetchToken,
    OrderByToken,
    GroupByToken,
    GroupByKeyToken,
    GroupBySumToken,
    GroupByCountToken,
    MoreLikeThisToken,
    CloseSubclauseToken,
    WhereToken,
    OpenSubclauseToken,
    TrueToken,
    NegateToken,
    QueryOperatorToken,
    CompareExchangeValueIncludesToken,
    HighlightingToken,
    ExplanationToken,
    TimingsToken,
    IntersectMarkerToken,
    DistinctToken,
    ShapeToken,
    SuggestToken,
)
from ravendb.documents.session.utils.document_query import DocumentQueryHelper
from ravendb.documents.session.utils.includes_util import IncludesUtil
from ravendb.tools.utils import Utils

_T = TypeVar("_T")
_TResult = TypeVar("_TResult")
_TProjection = TypeVar("_TProjection")
if TYPE_CHECKING:
    from ravendb.documents.store.definition import Lazy
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb.documents.session import InMemoryDocumentSessionOperations


class AbstractDocumentQuery(Generic[_T]):
    def __init__(
        self,
        object_type: Type[_T],
        session: "InMemoryDocumentSessionOperations",
        index_name: Union[None, str],
        collection_name: Union[None, str],
        is_group_by: bool,
        declare_tokens: Union[None, List[DeclareToken]],
        load_tokens: Union[None, List[LoadToken]],
        from_alias: Optional[str] = None,
        is_project_into: Optional[bool] = False,
    ):
        self._object_type = object_type
        self._root_types = {object_type}
        self._is_group_by = is_group_by
        self.__index_name = index_name
        self.__collection_name = collection_name
        self._from_token = FromToken.create(index_name, collection_name, from_alias)
        self._declare_tokens = declare_tokens
        self._load_tokens = load_tokens
        self._the_session = session
        self.__conventions = DocumentConventions() if session is None else session.conventions
        self.is_project_into = is_project_into if is_project_into is not None else False

        self._is_intersect: Union[None, bool] = None
        self.__is_in_more_like_this: Union[None, bool] = None
        self._includes_alias: Union[None, str] = None
        self._negate: Union[None, bool] = None
        self._query_raw: Union[None, str] = None
        self._query_parameters: Dict[str, object] = {}
        self.__alias_to_group_by_field_name: Dict[str, str] = {}

        self._document_includes: Set[str] = set()
        self._page_size: Union[None, int] = None
        self._start: Union[None, int] = None
        self.__current_clause_depth: int = 0

        self._select_tokens: List[QueryToken] = []
        self._fields_to_fetch_token: Union[None, FieldsToFetchToken] = None
        self._where_tokens: List[QueryToken] = []
        self._group_by_tokens: List[QueryToken] = []
        self._order_by_tokens: List[QueryToken] = []
        self._with_tokens: List[QueryToken] = []
        self._compare_exchange_includes_tokens: List[CompareExchangeValueIncludesToken] = []
        self._highlighting_tokens: List[HighlightingToken] = []
        self._query_highlightings: QueryHighlightings = QueryHighlightings()
        self._explanation_token: Union[None, ExplanationToken] = None
        self._explanations: Union[None, Explanations] = None

        self._before_query_executed_callback: List[Callable[[IndexQuery], None]] = []
        self._after_query_executed_callback: List[Callable[[QueryResult], None]] = [
            self.__update_stats_highlightings_and_explanations
        ]
        self._after_stream_executed_callback: List[Callable[[Dict], None]] = []

        self._query_stats = QueryStatistics()
        self._disable_entities_tracking: Union[None, bool] = None
        self._disable_caching: Union[None, bool] = None
        self._projection_behavior: Union[None, ProjectionBehavior] = None
        self.parameter_prefix = "p"
        self._query_timings: Union[None, QueryTimings] = None
        self._default_operator = QueryOperator.AND
        self._the_wait_for_non_stale_results: Union[None, bool] = None
        self._timeout: Union[None, datetime.timedelta] = None

        self._query_operation: Union[None, QueryOperation] = None

    @property
    def conventions(self) -> DocumentConventions:
        return self.__conventions

    @property
    def query_operation(self) -> QueryOperation:
        return self._query_operation

    @property
    def query_class(self) -> type:
        return self._object_type

    @property
    def index_name(self) -> str:
        return self.__index_name

    @property
    def collection_name(self) -> str:
        return self.__collection_name

    @property
    def is_distinct(self) -> bool:
        return len(self._select_tokens) > 0 and isinstance(self._select_tokens[0], DistinctToken)

    @property
    def session(self) -> "DocumentSession":
        self._the_session: "DocumentSession"
        return self._the_session

    @property
    def __default_timeout(self) -> datetime.timedelta:
        return self.conventions.wait_for_non_stale_results_timeout

    def _to_string(self) -> str:
        """
        Should be used only for testing
        Overrides of __str__ cause side effects during debugging
        @return: str
        """
        return self.__to_string(False)

    def _using_default_operator(self, operator: QueryOperator) -> None:
        if not self._where_tokens:
            raise RuntimeError("Default operator can only be set before any where clause is added")

        self._default_operator = operator

    def _wait_for_non_stale_results(self, wait_timeout: Union[None, datetime.timedelta]) -> None:
        # Graph queries may set this property multiple times
        if self._the_wait_for_non_stale_results:
            if self._timeout is None or (wait_timeout is not None and self._timeout.seconds < wait_timeout.seconds):
                self._timeout = wait_timeout
            return

        self._the_wait_for_non_stale_results = True
        self._timeout = wait_timeout or self.__default_timeout

    @property
    def lazy_query_operation(self) -> LazyQueryOperation:
        if self._query_operation is None:
            self._query_operation = self.initialize_query_operation()

        return LazyQueryOperation(
            self._object_type, self._the_session, self._query_operation, self._after_query_executed_callback
        )

    def initialize_query_operation(self) -> QueryOperation:
        self._the_session.before_query_invoke(
            BeforeQueryEventArgs(self._the_session, DocumentQueryCustomizationDelegate(self))
        )

        index_query = self.index_query

        return QueryOperation(
            self._the_session,
            self.__index_name,
            index_query,
            self._fields_to_fetch_token,
            self._disable_entities_tracking,
            False,
            False,
            self.is_project_into,
        )

    @property
    def index_query(self) -> IndexQuery:
        server_version = None
        if self._the_session is not None and self._the_session.advanced.request_executor is not None:
            server_version = self._the_session.advanced.request_executor.last_server_version

        compability_mode = server_version is not None and server_version < "4.2"

        query = self.__to_string(compability_mode)
        index_query = self._generate_index_query(query)
        self.invoke_before_query_executed(index_query)
        return index_query

    def _random_ordering(self, seed: Optional[str] = None) -> None:
        self.__assert_no_raw_query()
        self._no_caching()
        self._order_by_tokens.append(OrderByToken.random() if not seed else OrderByToken.create_random(seed))

    def _projection(self, projection_behavior):
        raise NotImplementedError()

    def _add_group_by_alias(self, field_name: str, projected_name: str) -> None:
        self.__alias_to_group_by_field_name[projected_name] = field_name

    def __assert_no_raw_query(self) -> None:
        if self._query_raw is not None:
            raise RuntimeError(
                "raw_query was called, cannot modify this query by calling on operations "
                "that would modify the query (such as where, select, order_by, group_by, etc)"
            )

    def _add_parameter(self, name: str, value: object) -> None:
        name = name.lstrip("$")
        if name in self._query_parameters:
            raise ValueError(f"The parameter {name} was already added")

        self._query_parameters[name] = value

    def _group_by(self, field_or_field_name: Union[GroupBy, str], *fields_or_field_names: Union[GroupBy, str]) -> None:
        field = field_or_field_name if isinstance(field_or_field_name, GroupBy) else GroupBy.field(field_or_field_name)
        fields = (
            (
                list(map(GroupBy.field, fields_or_field_names))
                if isinstance(fields_or_field_names[0], str)
                else list(fields_or_field_names)
            )
            if fields_or_field_names
            else []
        )
        fields.insert(0, field)

        if not self._from_token.dynamic:
            raise RuntimeError("group_by only works with dynamic queries")

        self.__assert_no_raw_query()
        self._is_group_by = True

        for item in fields:
            field_name = self.__ensure_valid_field_name(item.field, False)
            self._group_by_tokens.append(GroupByToken.create(field_name, item.method))

    def _group_by_key(self, field_name: str, projected_name: str = None) -> None:
        self.__assert_no_raw_query()
        self._is_group_by = True

        if projected_name is not None and projected_name in self.__alias_to_group_by_field_name:
            aliased_field_name = self.__alias_to_group_by_field_name[projected_name]
            if field_name is None or field_name.lower() == projected_name.lower():
                field_name = aliased_field_name
        elif field_name is not None and field_name in self.__alias_to_group_by_field_name:
            aliased_field_name = self.__alias_to_group_by_field_name[field_name]
            field_name = aliased_field_name

        self._select_tokens.append(GroupByKeyToken.create(field_name, projected_name))

    def _group_by_sum(self, field_name: str, projected_name: Optional[str] = None):
        self.__assert_no_raw_query()
        self._is_group_by = True

        field_name = self.__ensure_valid_field_name(field_name, False)
        self._select_tokens.append(GroupBySumToken.create(field_name, projected_name))

    def _group_by_count(self, projected_name: Optional[str] = None):
        self.__assert_no_raw_query()
        self._is_group_by = True

        self._select_tokens.append(GroupByCountToken.create(projected_name))

    def _where_true(self) -> None:
        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, None)

        tokens.append(TrueToken.instance())

    def _more_like_this(self) -> MoreLikeThisScope:
        self.__append_operator_if_needed(self._where_tokens)

        token = MoreLikeThisToken()
        self._where_tokens.append(token)

        self.__is_in_more_like_this = True

        def __action():
            self.__is_in_more_like_this = False

        return MoreLikeThisScope(token, self.__add_query_parameter, __action)

    def _include(self, path_or_include_builder: Union[str, IncludeBuilderBase]) -> None:
        if isinstance(path_or_include_builder, str):
            self._document_includes.add(path_or_include_builder)
        elif isinstance(path_or_include_builder, IncludeBuilderBase):
            includes = path_or_include_builder
            if includes is None:
                return
            if includes._documents_to_include is not None:
                self._document_includes.update(includes._documents_to_include)

            # todo: counters and time series includes
            # self._include_counters(includes.alias, includes.counters_to_include_by_source_path)
            # if includes.time_series_to_include_by_source_alias is not None:
            #     self._include_time_series(includes.alias, includes.time_series_to_include_by_source_alias)
            if includes._compare_exchange_values_to_include is not None:
                for compare_exchange_value in includes._compare_exchange_values_to_include:
                    self._compare_exchange_includes_tokens.append(
                        CompareExchangeValueIncludesToken.create(compare_exchange_value)
                    )

    def _take(self, count: int) -> None:
        self._page_size = count

    def _skip(self, count: int) -> None:
        self._start = count

    def _where_lucene(self, field_name: str, where_clause: str, exact: bool) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        options = WhereToken.WhereOptions(exact__from__to=(exact, None, None)) if exact else None
        where_token = WhereToken.create(
            WhereOperator.LUCENE, field_name, self.__add_query_parameter(where_clause), options
        )
        tokens.append(where_token)

    def _open_subclause(self) -> None:
        self.__current_clause_depth += 1

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, None)

        tokens.append(OpenSubclauseToken.create())

    def _close_subclause(self) -> None:
        self.__current_clause_depth -= 1

        tokens = self.__get_current_where_tokens()
        tokens.append(CloseSubclauseToken.create())

    def _where_equals(
        self,
        field_name__value_or_method__exact: Optional[Tuple[str, Union[MethodCall, object], Optional[bool]]] = None,
        where_params: Optional[WhereParams] = None,
    ) -> None:
        if not ((field_name__value_or_method__exact is not None) ^ (where_params is not None)):
            raise ValueError("Expected only one argument, got both or any")
        if field_name__value_or_method__exact:
            params = WhereParams()
            params.field_name = field_name__value_or_method__exact[0]
            params.value = field_name__value_or_method__exact[1]
            params.exact = (
                field_name__value_or_method__exact[2] if field_name__value_or_method__exact[2] is not None else False
            )
        elif where_params:
            params = where_params

        else:
            raise ValueError("Unexpected None value of the argument.")

        if self._negate:
            self._negate = False
            self._where_not_equals(where_params=params)
            return

        params.field_name = self.__ensure_valid_field_name(params.field_name, params.nested_path)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)

        if self.__if_value_is_method(WhereOperator.EQUALS, params, tokens):
            return

        transform_to_equal_value = self.__transform_value(params)
        add_query_parameter = self.__add_query_parameter(transform_to_equal_value)
        where_token = WhereToken.create(
            WhereOperator.EQUALS,
            params.field_name,
            add_query_parameter,
            WhereToken.WhereOptions(exact__from__to=(params.exact, None, None)),
        )
        tokens.append(where_token)

    def __if_value_is_method(self, op: WhereOperator, where_params: WhereParams, tokens: List[QueryToken]) -> bool:
        if isinstance(where_params.value, MethodCall):
            mc = where_params.value

            args = []
            for arg in mc.args:
                args.append(self.__add_query_parameter(arg))

            token: Union[None, WhereToken] = None
            object_type = type(mc)
            if object_type == CmpXchg:
                token = WhereToken.create(
                    op,
                    where_params.field_name,
                    None,
                    WhereToken.WhereOptions(
                        method_type__parameters__property__exact=(
                            WhereToken.MethodsType.CMP_X_CHG,
                            args,
                            mc.access_path,
                            where_params.exact,
                        )
                    ),
                )
            else:
                raise TypeError(f"Unknown method {object_type}")

            tokens.append(token)
            return True

        return False

    def _where_not_equals(
        self,
        field_name__value_or_method__exact: Optional[Tuple[str, Union[MethodCall, object], Optional[bool]]] = None,
        where_params: Optional[WhereParams] = None,
    ):
        is_where_params = where_params is not None
        if not ((field_name__value_or_method__exact is not None) ^ is_where_params):
            raise ValueError("Expected only one argument, got both or any")
        if not is_where_params:
            where_params = WhereParams()
            where_params.field_name = field_name__value_or_method__exact[0]
            where_params.value = field_name__value_or_method__exact[1]

            exact = field_name__value_or_method__exact[2] if len(field_name__value_or_method__exact) == 3 else False
            exact = False if exact is None else exact
            where_params.exact = exact

        if self._negate:
            self._negate = False
            self._where_equals(where_params)
            return

        transform_to_equal_value = self.__transform_value(where_params)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)

        where_params.field_name = self.__ensure_valid_field_name(where_params.field_name, where_params.nested_path)

        if self.__if_value_is_method(WhereOperator.NOT_EQUALS, where_params, tokens):
            return

        where_token = WhereToken.create(
            WhereOperator.NOT_EQUALS,
            where_params.field_name,
            self.__add_query_parameter(transform_to_equal_value),
            WhereToken.WhereOptions(exact__from__to=(where_params.exact, None, None)),
        )
        tokens.append(where_token)

    def _negate_next(self) -> None:
        self._negate = not self._negate

    def _where_in(self, field_name: str, values: Collection, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        where_token = WhereToken.create(
            WhereOperator.IN,
            field_name,
            self.__add_query_parameter(self.__transform_collection(field_name, Utils.unpack_collection(values))),
        )
        tokens.append(where_token)

    def _where_starts_with(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        where_params = WhereParams()
        where_params.field_name = field_name
        where_params.value = value
        where_params.allow_wildcards = True

        transform_to_equal_value = self.__transform_value(where_params)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)

        where_params.field_name = self.__ensure_valid_field_name(where_params.field_name, where_params.nested_path)
        self.__negate_if_needed(tokens, where_params.field_name)

        where_token = WhereToken.create(
            WhereOperator.STARTS_WITH,
            where_params.field_name,
            self.__add_query_parameter(transform_to_equal_value),
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_ends_with(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        where_params = WhereParams()
        where_params.field_name = field_name
        where_params.value = value
        where_params.allow_wildcards = True

        transform_to_equal_value = self.__transform_value(where_params)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)

        where_params.field_name = self.__ensure_valid_field_name(where_params.field_name, where_params.nested_path)
        self.__negate_if_needed(tokens, where_params.field_name)

        where_token = WhereToken.create(
            WhereOperator.ENDS_WITH,
            where_params.field_name,
            self.__add_query_parameter(transform_to_equal_value),
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_between(self, field_name: str, start: object, end: object, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        start_params = WhereParams()
        start_params.value = start
        start_params.field_name = field_name

        end_params = WhereParams()
        end_params.value = end
        end_params.field_name = field_name

        from_parameter_name = self.__add_query_parameter(
            "*" if start is None else self.__transform_value(start_params, True)
        )
        to_parameter_name = self.__add_query_parameter(
            "NULL" if end is None else self.__transform_value(end_params, True)
        )

        where_token = WhereToken.create(
            WhereOperator.BETWEEN,
            field_name,
            None,
            WhereToken.WhereOptions(exact__from__to=(exact, from_parameter_name, to_parameter_name)),
        )
        tokens.append(where_token)

    def _where_greater_than(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)
        where_params = WhereParams()
        where_params.value = value
        where_params.field_name = field_name

        parameter = self.__add_query_parameter("*" if value is None else self.__transform_value(where_params, True))
        where_token = WhereToken.create(
            WhereOperator.GREATER_THAN,
            field_name,
            parameter,
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_greater_than_or_equal(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)
        where_params = WhereParams()
        where_params.value = value
        where_params.field_name = field_name

        parameter = self.__add_query_parameter("*" if value is None else self.__transform_value(where_params, True))
        where_token = WhereToken.create(
            WhereOperator.GREATER_THAN_OR_EQUAL,
            field_name,
            parameter,
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_less_than(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)
        where_params = WhereParams()
        where_params.value = value
        where_params.field_name = field_name

        parameter = self.__add_query_parameter("*" if value is None else self.__transform_value(where_params, True))
        where_token = WhereToken.create(
            WhereOperator.LESS_THAN,
            field_name,
            parameter,
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_less_than_or_equal(self, field_name: str, value: object, exact: Optional[bool] = False) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        where_params = WhereParams()
        where_params.value = value
        where_params.field_name = field_name

        parameter = self.__add_query_parameter("NULL" if value is None else self.__transform_value(where_params, True))
        where_token = WhereToken.create(
            WhereOperator.LESS_THAN_OR_EQUAL,
            field_name,
            parameter,
            WhereToken.WhereOptions(exact__from__to=(exact, None, None)),
        )
        tokens.append(where_token)

    def _where_regex(self, field_name: str, pattern: str) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        where_params = WhereParams()
        where_params.value = pattern
        where_params.field_name = field_name

        parameter = self.__add_query_parameter(self.__transform_value(where_params))

        where_token = WhereToken.create(WhereOperator.REGEX, field_name, parameter)
        tokens.append(where_token)

    def _and_also(self) -> None:
        tokens = self.__get_current_where_tokens()
        if not tokens:
            return

        if isinstance(tokens[-1], QueryOperatorToken):
            raise TypeError("Cannot add AND, previous token was already an operator token")

        tokens.append(QueryOperatorToken.AND())

    def _or_else(self) -> None:
        tokens = self.__get_current_where_tokens()
        if len(tokens) == 0:
            return

        if isinstance(tokens[-1], QueryOperatorToken):
            raise RuntimeError("Cannot add OR, previous token was already an operator token")

        tokens.append(QueryOperatorToken.OR())

    def _boost(self, boost: float) -> None:
        if boost == 1.0:
            return

        if boost < 0.0:
            raise ValueError("Boost factor must be a non-negative number")

        tokens = self.__get_current_where_tokens()

        last = tokens[-1] if tokens else None

        if isinstance(last, WhereToken):
            where_token = last
            where_token.options.boost = boost
        elif isinstance(last, CloseSubclauseToken):
            close = last
            parameter = self.__add_query_parameter(boost)

            index = tokens.index(last)

            while last is not None and index > 0:
                index -= 1
                last = tokens[index]  # find the previous option

                if isinstance(last, OpenSubclauseToken):
                    open_token = last

                    open_token.boost_parameter_name = parameter
                    close.boost_parameter_name = parameter
                    return
        else:
            raise RuntimeError("Cannot apply boost")

    def _fuzzy(self, fuzzy: float) -> None:
        tokens = self.__get_current_where_tokens()
        if not tokens:
            raise RuntimeError("Fuzzy can only be used right after where clause")

        where_token = tokens[-1]
        if not isinstance(where_token, WhereToken):
            raise RuntimeError("Fuzzy can only be used right after where clause")

        if where_token.where_operator != WhereOperator.EQUALS:
            raise RuntimeError("Fuzzy can only be used right after where clause with equals operator")

        if fuzzy < 0.0 or fuzzy > 1.0:
            raise ValueError("Fuzzy distance must be between 0.0 and 1")

        where_token.options.fuzzy = fuzzy

    def _proximity(self, proximity: int) -> None:
        tokens = self.__get_current_where_tokens()
        if not tokens:
            raise RuntimeError("Proximity can only be used right after search clause")

        where_token = tokens[-1]
        if not isinstance(where_token, WhereToken):
            raise RuntimeError("Proximity can only be used right after search clause")

        if where_token.where_operator != WhereOperator.SEARCH:
            raise RuntimeError("Proximity can only be used right after search clause")

        if proximity < 1:
            raise ValueError("Proximity distance must be a positive number")

        where_token.options.proximity = proximity

    def _order_by(
        self, field: str, sorter_name_or_ordering_type: Optional[Union[str, OrderingType]] = OrderingType.STRING
    ) -> None:
        is_ordering_type = isinstance(sorter_name_or_ordering_type, OrderingType)
        if not is_ordering_type and sorter_name_or_ordering_type.isspace():
            raise ValueError("Sorter name cannot be None or whitespace")

        self.__assert_no_raw_query()
        f = self.__ensure_valid_field_name(field, False)
        self._order_by_tokens.append(OrderByToken.create_ascending(f, sorter_name_or_ordering_type))

    def _order_by_descending(
        self, field: str, sorter_name_or_ordering_type: Optional[Union[str, OrderingType]] = OrderingType.STRING
    ) -> None:
        is_ordering_type = isinstance(sorter_name_or_ordering_type, OrderingType)
        if not is_ordering_type and sorter_name_or_ordering_type.isspace():
            raise ValueError("Sorter name cannot be None or whitespace")

        self.__assert_no_raw_query()
        f = self.__ensure_valid_field_name(field, False)
        self._order_by_tokens.append(OrderByToken.create_descending(f, sorter_name_or_ordering_type))

    def _order_by_score(self) -> None:
        self.__assert_no_raw_query()

        self._order_by_tokens.append(OrderByToken.score_ascending())

    def _order_by_score_descending(self) -> None:
        self.__assert_no_raw_query()
        self._order_by_tokens.append(OrderByToken.score_descending())

    def _statistics(self, stats_callback: Callable[[QueryStatistics], None]) -> None:
        stats_callback(self._query_stats)

    def invoke_after_query_executed(self, result: QueryResult) -> None:
        for callback in self._after_query_executed_callback:
            callback(result)

    def invoke_before_query_executed(self, query: IndexQuery) -> None:
        for callback in self._before_query_executed_callback:
            callback(query)

    def invoke_after_stream_executed(self, result: dict) -> None:
        for callback in self._after_stream_executed_callback:
            callback(result)

    def _generate_index_query(self, query: str) -> IndexQuery:
        index_query = IndexQuery()
        index_query.query = query
        index_query.start = self._start
        index_query.wait_for_non_stale_results = self._the_wait_for_non_stale_results
        index_query.wait_for_non_stale_results_timeout = self._timeout
        index_query.query_parameters = self._query_parameters
        index_query.disable_caching = self._disable_caching
        index_query.projection_behavior = self._projection_behavior

        if self._page_size is not None:
            index_query.page_size = self._page_size

        return index_query

    def _search(self, field_name: str, search_terms: str, operator: SearchOperator) -> None:
        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)

        field_name = self.__ensure_valid_field_name(field_name, False)
        self.__negate_if_needed(tokens, field_name)

        where_token = WhereToken.create(
            WhereOperator.SEARCH,
            field_name,
            self.__add_query_parameter(search_terms),
            WhereToken.WhereOptions(search=operator),
        )
        tokens.append(where_token)

    def __to_string(self, compatibility_mode: bool) -> str:
        if self._query_raw is not None:
            return self._query_raw

        if self.__current_clause_depth != 0:
            raise RuntimeError(
                f"A clause was not closed correctly within this query, "
                f"current clause depth = {self.__current_clause_depth}"
            )

        query_text = []
        self.__build_declare(query_text)

        self.__build_from(query_text)
        self.__build_group_by(query_text)
        self.__build_where(query_text)
        self.__build_order_by(query_text)

        self.__build_load(query_text)
        self.__build_select(query_text)
        self.__build_include(query_text)

        if not compatibility_mode:
            self.__build_pagination(query_text)

        return "".join(query_text)

    def __build_with(self, query_text: List[str]) -> None:
        for with_token in self._with_tokens:
            with_token.write_to(query_text)
            query_text.append(os.linesep)

    def __build_pagination(self, query_text: List[str]) -> None:
        if (self._start is not None and self._start > 0) or self._page_size is not None:
            query_text.append(" limit $")
            query_text.append(self.__add_query_parameter(self._start or 0))
            query_text.append(", $")
            query_text.append(self.__add_query_parameter(self._page_size or 0))

    def __build_include(self, query_text: List[str]) -> None:
        if (
            not self._document_includes
            and not self._highlighting_tokens
            and self._explanation_token is None
            and self._query_timings is None
            and not self._compare_exchange_includes_tokens
            # todo: and self._counter_includes_tokens is None
            # todo: and self._time_series_includes_tokens is None
        ):
            return

        query_text.append(" include ")
        first = True
        for include in self._document_includes:
            if not first:
                query_text.append(",")
            first = False

            required, escaped_include = IncludesUtil.requires_quotes(include)
            if required:
                query_text.append("'")
                query_text.append(escaped_include)
                query_text.append("'")
            else:
                query_text.append(include)

        # todo: counters & time series
        # first = self.__write_include_tokens(self._counter_includes_tokens, first, query_text)
        # first = self.__write_include_tokens(self._time_series_includes_tokens, first, query_text)
        first = self.__write_include_tokens(self._compare_exchange_includes_tokens, first, query_text)
        first = self.__write_include_tokens(self._highlighting_tokens, first, query_text)

        if self._explanation_token is not None:
            if not first:
                query_text.append(",")

            first = False
            self._explanation_token.write_to(query_text)

        if self._query_timings is not None:
            if not first:
                query_text.append(",")
            first = False

            TimingsToken.instance().write_to(query_text)

    def __write_include_tokens(self, tokens: Collection[QueryToken], first: bool, query_text: List[str]) -> bool:
        if tokens is None:
            return first

        for token in tokens:
            if not first:
                query_text.append(",")
            first = False

            token.write_to(query_text)

        return first

    def _intersect(self) -> None:
        tokens = self.__get_current_where_tokens()
        if tokens:
            last = tokens[-1]
            if isinstance(last, (WhereToken, CloseSubclauseToken)):
                self._is_intersect = True

                tokens.append(IntersectMarkerToken.instance())
                return
        raise RuntimeError("Cannot add INTERSECT at this point.")

    def _where_exists(self, field_name: str) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, None)

        tokens.append(WhereToken.create(WhereOperator.EXISTS, field_name, None))

    def _contains_any(self, field_name: str, values: Collection) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        array = self.__transform_collection(field_name, Utils.unpack_collection(values))
        where_token = WhereToken.create(
            WhereOperator.IN,
            field_name,
            self.__add_query_parameter(array),
            WhereToken.WhereOptions(exact__from__to=(False, None, None)),
        )
        tokens.append(where_token)

    def _contains_all(self, field_name: str, values: Collection) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        array = self.__transform_collection(field_name, Utils.unpack_collection(values))

        if not array:
            tokens.append(TrueToken.instance())
            return

        where_token = WhereToken.create(WhereOperator.ALL_IN, field_name, self.__add_query_parameter(array))
        tokens.append(where_token)

    def _add_root_type(self, object_type: Type[_T]):
        self._root_types.add(object_type)

    def _distinct(self):
        if self.is_distinct:
            raise RuntimeError("The is already a distinct query")

        if len(self._select_tokens) == 0:
            self._select_tokens.append(DistinctToken.instance())
        else:
            self._select_tokens.insert(0, DistinctToken.instance())

    def __update_stats_highlightings_and_explanations(self, query_result: QueryResult) -> None:
        self._query_stats.update_query_stats(query_result)
        self._query_highlightings.update(query_result)
        if self._explanations is not None:
            self._explanations.update(query_result)
        if self._query_timings is not None:
            self._query_timings.update(query_result)

    def __build_select(self, writer: List[str]) -> None:
        if len(self._select_tokens) == 0:
            return

        writer.append(" select ")
        if len(self._select_tokens) == 1 and isinstance(self._select_tokens[0], DistinctToken):
            writer.append(" *")

            return

        for i in range(len(self._select_tokens)):
            token = self._select_tokens[i]
            if i > 0 and not isinstance(self._select_tokens[i - 1], DistinctToken):
                writer.append(",")

            DocumentQueryHelper.add_space_if_needed(self._select_tokens[i - 1] if i > 0 else None, token, writer)

            token.write_to(writer)

    def __build_from(self, writer: List[str]) -> None:
        self._from_token.write_to(writer)

    def __build_declare(self, writer: List[str]) -> None:
        if self._declare_tokens is None:
            return

        for token in self._declare_tokens:
            token.write_to(writer)

    def __build_load(self, writer: List[str]) -> None:
        if not self._load_tokens:
            return

        writer.append(" load ")
        for i in range(len(self._load_tokens)):
            if i != 0:
                writer.append(", ")

            self._load_tokens[i].write_to(writer)

    def __build_where(self, writer: List[str]):
        if len(self._where_tokens) == 0:
            return

        writer.append(" where ")

        if self._is_intersect:
            writer.append("intersect(")

        for i in range(len(self._where_tokens)):
            DocumentQueryHelper.add_space_if_needed(
                self._where_tokens[i - 1] if i > 0 else None, self._where_tokens[i], writer
            )
            self._where_tokens[i].write_to(writer)

        if self._is_intersect:
            writer.append(") ")

    def __build_group_by(self, writer: List[str]) -> None:
        if len(self._group_by_tokens) == 0:
            return

        writer.append(" group by ")
        first = True

        for token in self._group_by_tokens:
            if not first:
                writer.append(", ")

            token.write_to(writer)
            first = False

    def __build_order_by(self, writer: List[str]) -> None:
        if len(self._order_by_tokens) == 0:
            return

        writer.append(" order by ")

        first = True

        for token in self._order_by_tokens:
            if not first:
                writer.append(", ")

            token.write_to(writer)
            first = False

    def __append_operator_if_needed(self, tokens: List[QueryToken]) -> None:
        self.__assert_no_raw_query()

        if not tokens:
            return

        last_token = tokens[-1]
        if not isinstance(last_token, (WhereToken, CloseSubclauseToken)):
            return

        last_where: Optional[WhereToken] = None
        for i in range(len(tokens) - 1, -1, -1):
            if isinstance(tokens[i], WhereToken):
                last_where = tokens[i]
                break

        token = QueryOperatorToken.AND() if self._default_operator == QueryOperator.AND else QueryOperatorToken.OR()

        if last_where is not None and last_where.options.search_operator is not None:
            token = QueryOperatorToken.OR()

        tokens.append(token)

    def __transform_collection(self, field_name: str, values: List) -> List:
        result = []
        for value in values:
            if Utils.check_if_collection_but_not_str(value):
                result.extend(self.__transform_collection(field_name, value))
            else:
                nested_where_params = WhereParams()
                nested_where_params.allow_wildcards = True
                nested_where_params.field_name = field_name
                nested_where_params.value = value

                result.append(self.__transform_value(nested_where_params))
        return result

    def __negate_if_needed(self, tokens: List[QueryToken], field_name: Union[None, str]) -> None:
        if not self._negate:
            return

        self._negate = False

        if not tokens or isinstance(tokens[-1], OpenSubclauseToken):
            if field_name is not None:
                self._where_exists(field_name)
            else:
                self._where_true()
            self._and_also()

        tokens.append(NegateToken.instance())

    def __ensure_valid_field_name(self, field_name, is_nested_path) -> str:
        if not self._the_session or self._the_session.conventions is None or is_nested_path or self._is_group_by:
            return QueryFieldUtil.escape_if_necessary(field_name, is_nested_path)

        # todo: root types, checking if identity property name isnt the same compared to field_name
        return QueryFieldUtil.escape_if_necessary(field_name)

    def __transform_value(self, where_params: WhereParams, for_range: Optional[bool] = False) -> object:
        if where_params.value is None:
            return None

        if "" == where_params.value:
            return ""

        success, obj = self.__conventions.try_convert_value_to_object_for_query(
            where_params.field_name, where_params.value, for_range
        )
        if success:
            return obj

        if isinstance(where_params.value, (datetime.datetime, str, int, float, bool, enum.Enum)):
            return where_params.value
        if isinstance(where_params.value, datetime.timedelta):
            return where_params.value.total_seconds() * 10000000
        return where_params.value

    def __add_query_parameter(self, value: object) -> str:
        parameter_name = f"{self.parameter_prefix}{len(self._query_parameters)}"
        self._query_parameters[parameter_name] = value
        return parameter_name

    def __get_current_where_tokens(self):
        if not self.__is_in_more_like_this:
            return self._where_tokens

        if not self._where_tokens:
            raise RuntimeError("Cannot get MoreLikeThisToken because there are no where token specified")

        last_token = self._where_tokens[-1]

        if isinstance(last_token, MoreLikeThisToken):
            more_like_this_token: MoreLikeThisToken = last_token
            return more_like_this_token.where_tokens
        else:
            raise RuntimeError("Last token is not MoreLikeThisToken")

    def _update_fields_to_fetch_token(self, fields_to_fetch: FieldsToFetchToken) -> None:
        self._fields_to_fetch_token = fields_to_fetch

        if not self._select_tokens:
            self._select_tokens.append(fields_to_fetch)
        else:
            fetch_tokens = list(filter(lambda x: isinstance(x, FieldsToFetchToken), self._select_tokens))
            fetch_token = fetch_tokens[0] if fetch_tokens else None

            if fetch_token is not None:
                idx = self._select_tokens.index(fetch_token)
                self._select_tokens[idx] = fields_to_fetch
            else:
                self._select_tokens.append(fields_to_fetch)

    def add_from_alias_to_where_tokens(self, from_alias: str) -> None:
        if not from_alias:
            raise RuntimeError("Alias cannot be None or empty")

        tokens = self.__get_current_where_tokens()
        for token in tokens:
            if isinstance(token, WhereToken):
                token.add_alias(from_alias)

    def add_alias_to_includes_tokens(self, from_alias: str) -> str:
        if self._includes_alias is None:
            return from_alias

        if from_alias is None:
            from_alias = self._includes_alias
            self.add_from_alias_to_where_tokens(from_alias)

        # todo: counter and time series tokens

        return from_alias

    @staticmethod
    def _get_source_alias_if_exists(object_type: type, query_data: QueryData, fields: List[str]) -> Union[None, str]:
        if len(fields) != 1 or fields[0] is None:
            return None
        try:
            index_of = fields[0].index(".")
        except ValueError:
            return None

        possible_alias = fields[0][0:index_of]
        if query_data.from_alias is not None and query_data.from_alias == possible_alias:
            return possible_alias

        if not query_data.load_tokens:
            return None

        if not any([token.alias == possible_alias for token in query_data.load_tokens]):
            return None

        return possible_alias

    # todo: def _create_time_series_query_data(time_series_query: Callable[[TimeSeriesQueryBuilder], None])
    #  -> QueryData:

    def _add_before_query_executed_listener(self, action: Callable[[IndexQuery], None]) -> None:
        self._before_query_executed_callback.append(action)

    def _remove_before_query_executed_listener(self, action: Callable[[IndexQuery], None]) -> None:
        self._before_query_executed_callback.remove(action)

    def _add_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> None:
        self._after_query_executed_callback.append(action)

    def _remove_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> None:
        self._remove_after_query_executed_listener(action)

    def _add_after_stream_executed_listener(self, action: Callable[[dict], None]) -> None:
        self._after_stream_executed_callback.append(action)

    def _remove_after_stream_executed_listener(self, action: Callable[[dict], None]) -> None:
        self._remove_after_stream_executed_listener(action)

    def _no_tracking(self) -> None:
        self._disable_entities_tracking = True

    def _no_caching(self) -> None:
        self._disable_caching = True

    def _include_timings(self, timings_callback: Callable[[QueryTimings], None] = None) -> None:
        if self._query_timings is not None:
            timings_callback(self._query_timings)
            return
        self._query_timings = QueryTimings()
        timings_callback(self._query_timings)

    def _highlight(
        self,
        field_name: str,
        fragment_length: int,
        fragment_count: int,
        options: HighlightingOptions,
        highlightings_callback: Callable[[Highlightings], None],
    ) -> None:
        highlightings_callback(self._query_highlightings.add(field_name))
        options_parameter_name = self.__add_query_parameter(options.to_json()) if options is not None else None
        self._highlighting_tokens.append(
            HighlightingToken.create(field_name, fragment_length, fragment_count, options_parameter_name)
        )

    def _within_radius_of(
        self,
        field_name: str,
        radius: float,
        latitude: float,
        longitude: float,
        radius_units: SpatialUnits,
        dist_error_percent: float,
    ) -> None:
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        where_token = WhereToken.create(
            WhereOperator.SPATIAL_WITHIN,
            field_name,
            None,
            WhereToken.WhereOptions(
                shape__distance=(
                    ShapeToken.circle(
                        self.__add_query_parameter(radius),
                        self.__add_query_parameter(latitude),
                        self.__add_query_parameter(longitude),
                        radius_units,
                    ),
                    dist_error_percent,
                ),
            ),
        )
        tokens.append(where_token)

    def __assert_is_dynamic_query(self, dynamic_field: DynamicSpatialField, method_name: str) -> None:
        if not self._from_token.dynamic:
            raise RuntimeError(
                f"Cannot execute query method '{method_name}'. "
                f"Field '{dynamic_field.to_field(self.__ensure_valid_field_name)}' cannot be used when "
                f"static index '{self._from_token.index_name}' is queried. Dynamic spatial fields can "
                f"only be used with dynamic queries, for static index queries please use valid spatial "
                f"fields defined in index definition."
            )

    def _spatial_with_criteria(self, field_or_field_name: Union[str, DynamicSpatialField], criteria: SpatialCriteria):
        is_string = isinstance(field_or_field_name, str)
        if is_string:
            field_or_field_name = self.__ensure_valid_field_name(field_or_field_name, False)
        elif issubclass(field_or_field_name.__class__, DynamicSpatialField):
            self.__assert_is_dynamic_query(field_or_field_name, "spatial")
        else:
            raise TypeError(
                "Incorrect type for 'field_or_field_name' in the _spatial_with_criteria method. "
                f"Pass str or DynamicSpatialField. Type is '{field_or_field_name.__class__.__name__}'"
            )

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, None)

        tokens.append(
            criteria.to_query_token(
                field_or_field_name if is_string else field_or_field_name.to_field(self.__ensure_valid_field_name),
                self.__add_query_parameter,
            )
        )

    def _spatial(
        self,
        field_name: str,
        shape_wkt: str,
        relation: SpatialRelation,
        units: SpatialUnits,
        dist_error_percent: float,
    ):
        field_name = self.__ensure_valid_field_name(field_name, False)

        tokens = self.__get_current_where_tokens()
        self.__append_operator_if_needed(tokens)
        self.__negate_if_needed(tokens, field_name)

        wkt_token = ShapeToken.wkt(self.__add_query_parameter(shape_wkt), units)

        if relation == SpatialRelation.WITHIN:
            where_operator = WhereOperator.SPATIAL_WITHIN
        elif relation == SpatialRelation.CONTAINS:
            where_operator = WhereOperator.SPATIAL_CONTAINS
        elif relation == SpatialRelation.DISJOINT:
            where_operator = WhereOperator.SPATIAL_DISJOINT
        elif relation == SpatialRelation.INTERSECTS:
            where_operator = WhereOperator.SPATIAL_INTERSECTS
        else:
            raise ValueError()

        tokens.append(
            WhereToken.create(
                where_operator,
                field_name,
                None,
                WhereToken.WhereOptions(shape__distance=(wkt_token, dist_error_percent)),
            )
        )
        return

    def _order_by_distance(
        self,
        field_or_field_name: Union[DynamicSpatialField, str],
        latitude: float,
        longitude: float,
        round_factor: float = 0,
    ) -> None:
        is_dynamic_field = isinstance(field_or_field_name, DynamicSpatialField)
        if is_dynamic_field:
            if field_or_field_name is None:
                raise ValueError("Field cannot be None")
            self.__assert_is_dynamic_query(field_or_field_name, "orderByDistance")
            round_factor = field_or_field_name.round_factor
            field_name = f"'{field_or_field_name.to_field(self.__ensure_valid_field_name)}'"
        else:
            field_name = field_or_field_name

        round_factor_parameter_name = None if round_factor == 0 else self.__add_query_parameter(round_factor)

        self._order_by_tokens.append(
            OrderByToken.create_distance_ascending(
                field_name,
                self.__add_query_parameter(latitude),
                self.__add_query_parameter(longitude),
                round_factor_parameter_name,
            )
        )

    def _order_by_distance_wkt(
        self,
        field_or_field_name: Union[DynamicSpatialField, str],
        shape_wkt: str,
        round_factor: float = 0,
    ) -> None:
        is_dynamic_field = isinstance(field_or_field_name, DynamicSpatialField)
        if is_dynamic_field:
            if field_or_field_name is None:
                raise ValueError("Field cannot be None")
            self.__assert_is_dynamic_query(field_or_field_name, "orderByDistance")
            round_factor = field_or_field_name.round_factor
            field_name = f"'{field_or_field_name.to_field(self.__ensure_valid_field_name)}'"
        else:
            round_factor = self.__add_query_parameter(round_factor) if round_factor != 0 else None
            field_name = field_or_field_name

        round_factor_parameter_name = None if round_factor == 0 else self.__add_query_parameter(round_factor)
        self._order_by_tokens.append(
            OrderByToken.create_distance_ascending_wkt(field_name, shape_wkt, round_factor_parameter_name)
        )

    def _order_by_distance_descending(
        self,
        field_or_field_name: Union[DynamicSpatialField, str],
        latitude: float,
        longitude: float,
        round_factor: float = 0,
    ) -> None:
        is_dynamic_field = isinstance(field_or_field_name, DynamicSpatialField)

        if is_dynamic_field:
            if field_or_field_name is None:
                raise ValueError("Field cannot be None")
            self.__assert_is_dynamic_query(field_or_field_name, "orderByDistanceDescending")
            round_factor = field_or_field_name.round_factor
            field_name = f"'{field_or_field_name.to_field(self.__ensure_valid_field_name)}'"
        else:
            round_factor = self.__add_query_parameter(round_factor) if round_factor != 0 else None
            field_name = field_or_field_name

        round_factor_parameter_name = None if round_factor == 0 else self.__add_query_parameter(round_factor)

        self._order_by_tokens.append(
            OrderByToken.create_distance_descending(
                field_name,
                self.__add_query_parameter(latitude),
                self.__add_query_parameter(longitude),
                round_factor_parameter_name,
            )
        )

    def _order_by_distance_descending_wkt(
        self,
        field_or_field_name: Union[DynamicSpatialField, str],
        shape_wkt: str,
        round_factor: float = 0,
    ):
        is_dynamic_field = isinstance(field_or_field_name, DynamicSpatialField)

        if is_dynamic_field:
            if field_or_field_name is None:
                raise ValueError("Field cannot be None")
            self.__assert_is_dynamic_query(field_or_field_name, "orderByDistanceDescending")
            round_factor = field_or_field_name.round_factor
            field_name = f"'{field_or_field_name.to_field(self.__ensure_valid_field_name)}'"
        else:
            round_factor = self.__add_query_parameter(round_factor) if round_factor != 0 else None
            field_name = field_or_field_name
        round_factor_parameter_name = None if round_factor == 0 else self.__add_query_parameter(round_factor)

        self._order_by_tokens.append(
            OrderByToken.create_distance_descending_wkt(field_name, shape_wkt, round_factor_parameter_name)
        )

    def _init_sync(self) -> None:
        if self._query_operation is not None:
            return

        self._query_operation = self.initialize_query_operation()
        self.__execute_actual_query()

    def __execute_actual_query(self) -> None:
        self._query_operation.enter_query_context()  # todo: improve with context manager
        command = self._query_operation.create_request()
        self._the_session.advanced.request_executor.execute_command(command, self._the_session.session_info)
        self._query_operation.set_result(command.result)
        self.invoke_after_query_executed(self._query_operation.current_query_results)

    def __iter__(self) -> Iterator[_T]:
        return self.__execute_query_operation(None).__iter__()

    def __execute_query_operation(self, take: Union[None, int]) -> List[_T]:
        self.__execute_query_operation_internal(take)

        return self._query_operation.complete(self._object_type)

    def __execute_query_operation_internal(self, take: int) -> None:
        if take is not None and (self._page_size is None or self._page_size > take):
            self._take(take)

        self._init_sync()

    def get_query_result(self):
        self._init_sync()

        return self._query_operation.current_query_results.create_snapshot()

    def _aggregate_by(self, facet: FacetBase) -> None:
        for token in self._select_tokens:
            if isinstance(token, FacetToken):
                continue

            raise RuntimeError(
                f"Aggregation query can select only facets while it got {token.__class__.__name__} token"
            )

        self._select_tokens.append(FacetToken.create_from_facet_base(facet, self.__add_query_parameter))

    def _aggregate_using(self, facet_setup_document_id: str) -> None:
        self._select_tokens.append(FacetToken.create_from_facet_setup_document_id(facet_setup_document_id))

    def lazily(self, on_eval: Callable[[List[_T]], None] = None) -> Lazy[List[_T]]:
        lazy_query_operation = self.lazy_query_operation

        self._the_session: "DocumentSession"
        return self._the_session.add_lazy_operation(list, lazy_query_operation, on_eval)

    def count_lazily(self) -> Lazy[int]:
        if self._query_operation is None:
            self._take(0)
            self._query_operation = self.initialize_query_operation()

        lazy_query_operation = LazyQueryOperation(
            self._object_type, self._the_session, self._query_operation, self._after_query_executed_callback
        )
        self._the_session: "DocumentSession"
        return self._the_session.add_lazy_count_operation(self.lazy_query_operation)

    def _suggest_using(self, suggestion: Union[None, SuggestionBase]) -> None:
        if suggestion is None:
            raise ValueError("suggestion cannot be None")

        self.__assert_can_suggest(suggestion)

        if isinstance(suggestion, SuggestionWithTerm):
            term = suggestion
            token = SuggestToken.create(
                term.field,
                term.display_field,
                self.__add_query_parameter(term.term),
                self.__get_options_parameter_name(term.options),
            )
        elif isinstance(suggestion, SuggestionWithTerms):
            terms = suggestion
            token = SuggestToken.create(
                terms.field,
                terms.display_field,
                self.__add_query_parameter(terms.terms),
                self.__get_options_parameter_name(terms.options),
            )
        else:
            raise NotImplementedError(f"Unknown type of suggestion: {suggestion.__class__.__name__}")

        self._select_tokens.append(token)

    def __get_options_parameter_name(self, options: SuggestionOptions):
        return (
            self.__add_query_parameter(options)
            if options is not None and options != SuggestionOptions.default_options()
            else None
        )

    def __assert_can_suggest(self, suggestion: SuggestionBase) -> None:
        if self._where_tokens:
            raise RuntimeError("Cannot add suggest when WHERE statements are present")

        if self._select_tokens:
            last_token = self._select_tokens[-1]
            if isinstance(last_token, SuggestToken):
                if last_token.field_name == suggestion.field:
                    raise RuntimeError("Cannot add suggest for the same field again.")

            else:
                raise RuntimeError("Cannot add suggest when SELECT statements are present.")

        if self._order_by_tokens:
            raise RuntimeError("Cannot add suggest when ORDER BY statements are present.")

    def _include_explanations(
        self, options: ExplanationOptions, explanations_callback: Callable[[Explanations], None]
    ) -> None:
        if self._explanation_token is not None:
            raise RuntimeError("Duplicate IncludeExplanations method calls are forbidden.")

        options_parameter_name = self.__add_query_parameter(options) if options is not None else None
        self._explanation_token = ExplanationToken.create(options_parameter_name)
        self._explanations = Explanations()
        explanations_callback(self._explanations)

    # todo: counters
    def _include_counters(self, alias: str, counter_to_include_by_doc_id: Dict[str, Tuple[bool, Set[str]]]) -> None:
        raise NotImplementedError()

    # todo: time series
    # def _include_time_series(self, alias:str, time_series_to_include:Dict[str,Set[AbstractTimeSeriesRange]]) -> None:
    # pass


class DocumentQuery(Generic[_T], AbstractDocumentQuery[_T]):
    def __init__(
        self,
        object_type: Type[_T],
        session: InMemoryDocumentSessionOperations,
        index_name: str,
        collection_name: str,
        is_group_by: bool,
        declare_tokens: List[DeclareToken] = None,
        load_tokens: List[LoadToken] = None,
        from_alias: str = None,
        is_project_into: bool = None,
    ):
        super().__init__(
            object_type,
            session,
            index_name,
            collection_name,
            is_group_by,
            declare_tokens,
            load_tokens,
            from_alias,
            is_project_into,
        )

    # todo: selectTimeSeries

    def distinct(self) -> DocumentQuery[_T]:
        self._distinct()
        return self

    def order_by_score(self) -> DocumentQuery[_T]:
        self._order_by_score()
        return self

    def order_by_score_descending(self) -> DocumentQuery[_T]:
        self._order_by_score_descending()
        return self

    def include_explanations(
        self, options: Optional[ExplanationOptions] = None, explanations_callback: Callable[[Explanations], None] = None
    ) -> DocumentQuery[_T]:
        self._include_explanations(options, explanations_callback)
        return self

    def timings(self, timings_callback: Callable[[QueryTimings], None]) -> DocumentQuery[_T]:
        self._include_timings(timings_callback)
        return self

    def select_fields_query_data(
        self, projection_class: Type[_TProjection], query_data: QueryData
    ) -> DocumentQuery[_TProjection]:
        query_data.project_into = True
        return self.create_document_query_internal(projection_class, query_data)

    def select_fields(
        self,
        projection_class: Type[_TProjection],
        *fields: str,
        projection_behavior: Optional[ProjectionBehavior] = ProjectionBehavior.DEFAULT,
    ) -> DocumentQuery[_TProjection]:
        if not fields:
            fields = list(Utils.get_class_fields(projection_class))
        fields = list(fields)
        query_data = QueryData(fields, copy(fields))
        query_data.project_into = True
        query_data.projection_behavior = projection_behavior

        select_fields = self.select_fields_query_data(projection_class, query_data=query_data)
        return select_fields

    def wait_for_non_stale_results(self, wait_timeout: datetime.timedelta = None) -> DocumentQuery[_T]:
        self._wait_for_non_stale_results(wait_timeout)
        return self

    def add_parameter(self, name: str, value: object) -> DocumentQuery[_T]:
        self._add_parameter(name, value)
        return self

    def add_order(
        self, field_name: str, descending: bool, ordering: OrderingType = OrderingType.STRING
    ) -> DocumentQuery[_T]:
        if descending:
            self._order_by_descending(field_name, ordering)
        else:
            self._order_by(field_name, ordering)
        return self

    def add_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> DocumentQuery[_T]:
        self._add_after_query_executed_listener(action)
        return self

    def remove_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> DocumentQuery[_T]:
        self._remove_after_query_executed_listener(action)
        return self

    def add_after_stream_executed_listener(self, action: Callable[[dict], None]) -> DocumentQuery[_T]:
        self._add_after_stream_executed_listener(action)
        return self

    def remove_after_stream_executed_listener(self, action: Callable[[dict], None]) -> DocumentQuery[_T]:
        self._remove_after_stream_executed_listener(action)
        return self

    def open_subclause(self) -> DocumentQuery[_T]:
        self._open_subclause()
        return self

    def close_subclause(self) -> DocumentQuery[_T]:
        self._close_subclause()
        return self

    def negate_next(self) -> DocumentQuery[_T]:
        self._negate_next()
        return self

    def search(self, field_name: str, search_terms: str, operator: SearchOperator = None) -> DocumentQuery[_T]:
        self._search(field_name, search_terms, operator)
        return self

    def intersect(self) -> DocumentQuery[_T]:
        self._intersect()
        return self

    def contains_any(self, field_name: str, values: Collection) -> DocumentQuery[_T]:
        self._contains_any(field_name, values)
        return self

    def contains_all(self, field_name: str, values: Collection) -> DocumentQuery[_T]:
        self._contains_all(field_name, values)
        return self

    def statistics(self, stats_callback: Callable[[QueryStatistics], None]) -> DocumentQuery[_T]:
        self._statistics(stats_callback)
        return self

    def using_default_operator(self, query_operator: QueryOperator) -> DocumentQuery[_T]:
        self._using_default_operator(query_operator)
        return self

    def no_tracking(self) -> DocumentQuery[_T]:
        self._no_tracking()
        return self

    def no_caching(self) -> DocumentQuery[_T]:
        self._no_caching()
        return self

    def include(
        self, path_or_include_builder_callback: Union[str, Callable[[QueryIncludeBuilder], None]]
    ) -> DocumentQuery[_T]:
        if not isinstance(path_or_include_builder_callback, str):
            include_builder = QueryIncludeBuilder(self.conventions)
            path_or_include_builder_callback(include_builder)
            self._include(include_builder)
            return self
        self._include(path_or_include_builder_callback)
        return self

    def not_(self) -> DocumentQuery[_T]:
        self.negate_next()
        return self

    def take(self, count: int) -> DocumentQuery[_T]:
        self._take(count)
        return self

    def skip(self, count: int) -> DocumentQuery[_T]:
        self._skip(count)
        return self

    def first(self) -> _T:
        return list(self.take(1))[0]

    def count(self) -> int:
        self._take(0)
        query_result = self.get_query_result()
        return query_result.total_results

    def single(self) -> _T:
        result = list(self.take(2))
        if len(result) != 1:
            raise ValueError(f"Expected signle result, got: {len(result)} ")
        return result[0]

    def where_lucene(self, field_name: str, where_clause: str, exact: bool = False) -> DocumentQuery[_T]:
        self._where_lucene(field_name, where_clause, exact)
        return self

    def where(self, exact: Optional[bool] = False, **kwargs: Union[object, Collection]) -> DocumentQuery[_T]:
        first = True
        for field_name, value in kwargs.items():
            if not first:
                self.or_else()
            first = False
            if Utils.check_if_collection_but_not_str(value):
                self.where_in(field_name, value, exact)
            else:
                self.where_equals(field_name, value, exact)
        return self

    def where_equals(
        self, field_name: str, value_or_method: Union[object, MethodCall], exact: Optional[bool] = None
    ) -> DocumentQuery[_T]:
        self._where_equals(field_name__value_or_method__exact=(field_name, value_or_method, exact))
        return self

    def where_equals_params(self, where_params: WhereParams) -> DocumentQuery[_T]:
        self._where_equals(where_params=where_params)
        return self

    def where_not_equals(
        self, field_name: str, value_or_method: Union[object, MethodCall], exact: Optional[bool] = None
    ) -> DocumentQuery[_T]:
        self._where_not_equals(field_name__value_or_method__exact=(field_name, value_or_method, exact))
        return self

    def where_not_none(self, field_name: str) -> DocumentQuery[_T]:
        return self.where_not_equals(field_name, None)

    def where_not_equals_params(self, where_params: WhereParams) -> DocumentQuery[_T]:
        self._where_not_equals(where_params=where_params)
        return self

    def where_in(self, field_name: str, values: Collection, exact: Optional[bool] = None) -> DocumentQuery[_T]:
        self._where_in(field_name, values, exact)
        return self

    def where_starts_with(self, field_name: str, value: object, exact: Optional[bool] = None) -> DocumentQuery[_T]:
        self._where_starts_with(field_name, value, exact)
        return self

    def where_ends_with(self, field_name: str, value: object, exact: Optional[bool] = None) -> DocumentQuery[_T]:
        self._where_ends_with(field_name, value, exact)
        return self

    def where_between(
        self, field_name: str, start: object, end: object, exact: Optional[bool] = False
    ) -> DocumentQuery[_T]:
        self._where_between(field_name, start, end, exact)
        return self

    def where_greater_than(self, field_name: str, value: object, exact: Optional[bool] = False) -> DocumentQuery[_T]:
        self._where_greater_than(field_name, value, exact)
        return self

    def where_greater_than_or_equal(
        self, field_name: str, value: object, exact: Optional[bool] = False
    ) -> DocumentQuery[_T]:
        self._where_greater_than_or_equal(field_name, value, exact)
        return self

    def where_less_than(self, field_name: str, value: object, exact: Optional[bool] = False) -> DocumentQuery[_T]:
        self._where_less_than(field_name, value, exact)
        return self

    def where_less_than_or_equal(
        self, field_name: str, value: object, exact: Optional[bool] = False
    ) -> DocumentQuery[_T]:
        self._where_less_than_or_equal(field_name, value, exact)
        return self

    def where_exists(self, field_name: str) -> DocumentQuery[_T]:
        self._where_exists(field_name)
        return self

    def where_regex(self, field_name: str, pattern: str) -> DocumentQuery[_T]:
        self._where_regex(field_name, pattern)
        return self

    def and_also(self) -> DocumentQuery[_T]:
        self._and_also()
        return self

    def or_else(self) -> DocumentQuery[_T]:
        self._or_else()
        return self

    def boost(self, boost: float) -> DocumentQuery[_T]:
        self._boost(boost)
        return self

    def fuzzy(self, fuzzy: float) -> DocumentQuery[_T]:
        self._fuzzy(fuzzy)
        return self

    def proximity(self, proximity: int) -> DocumentQuery[_T]:
        self._proximity(proximity)
        return self

    def random_ordering(self, seed: str = None) -> DocumentQuery[_T]:
        self._random_ordering(seed)
        return self

    def group_by(self, field_name: Union[GroupBy, str], *field_names: Union[GroupBy, str]) -> GroupByDocumentQuery[_T]:
        self._group_by(field_name, *field_names)
        return GroupByDocumentQuery(self)

    def of_type(self, t_result_class: Type[_TResult]) -> DocumentQuery[_TResult]:
        return self.create_document_query_internal(t_result_class)

    def order_by(
        self, field: str, sorter_name_or_ordering_type: Union[str, OrderingType] = OrderingType.STRING
    ) -> DocumentQuery[_T]:
        self._order_by(field, sorter_name_or_ordering_type)
        return self

    def order_by_descending(
        self, field: str, sorter_name_or_ordering_type: Union[str, OrderingType] = OrderingType.STRING
    ) -> DocumentQuery[_T]:
        self._order_by_descending(field, sorter_name_or_ordering_type)
        return self

    def add_before_query_executed_listener(self, action: Callable[[IndexQuery], None]) -> DocumentQuery[_T]:
        self._add_before_query_executed_listener(action)
        return self

    def remove_before_query_executed_listener(self, action: Callable[[IndexQuery], None]) -> DocumentQuery[_T]:
        self._remove_before_query_executed_listener(action)
        return self

    def create_document_query_internal(
        self, result_class: Type[_TResult], query_data: Optional[QueryData] = None
    ) -> DocumentQuery[_TResult]:
        if query_data:
            fields = query_data.fields

            if not self._is_group_by:
                # todo: conventions.getIdentityProperty(result_class) - now I assume that identity property is always Id
                if "Id" in fields:
                    fields[fields.index("Id")] = constants.Documents.Indexing.Fields.DOCUMENT_ID_FIELD_NAME

            source_alias = self._get_source_alias_if_exists(result_class, query_data, fields)
            new_fields_to_fetch = FieldsToFetchToken.create(
                fields, query_data.projections, query_data.is_custom_function, source_alias
            )
        else:
            new_fields_to_fetch = None

        if new_fields_to_fetch is not None:
            self._update_fields_to_fetch_token(new_fields_to_fetch)

        query = DocumentQuery(
            result_class,
            self._the_session,
            self.index_name,
            self.collection_name,
            self._is_group_by,
            query_data.declare_tokens if query_data is not None else None,
            query_data.load_tokens if query_data is not None else None,
            query_data.from_alias if query_data is not None else None,
            query_data.project_into if query_data is not None else None,
        )

        query._query_raw = self._query_raw
        query._page_size = self._page_size
        query._select_tokens = list(self._select_tokens)
        query._fields_to_fetch_token = self._fields_to_fetch_token
        query._where_tokens = list(self._where_tokens)
        query._order_by_tokens = list(self._order_by_tokens)
        query._group_by_tokens = list(self._group_by_tokens)
        query._query_parameters = Parameters(self._query_parameters)
        query._start = self._start
        query._timeout = self._timeout
        query._query_stats = self._query_stats
        query._the_wait_for_non_stale_results = self._the_wait_for_non_stale_results
        query._negate = self._negate
        query._document_includes = self._document_includes
        # todo: self._counter_includes_tokens = self._counter_includes_tokens
        # todo: self._time_series_includes_tokens = self._time_series_includes_tokens
        query._compare_exchange_includes_tokens = self._compare_exchange_includes_tokens
        query._root_types = {self._object_type}
        query._before_query_executed_callback = self._before_query_executed_callback
        query._after_query_executed_callback = self._after_query_executed_callback
        query._after_stream_executed_callback = self._after_stream_executed_callback
        query._highlighting_tokens = self._highlighting_tokens
        query._query_highlightings = self._query_highlightings
        query._disable_entities_tracking = self._disable_entities_tracking
        query._disable_caching = self._disable_caching
        query._projection_behavior = (
            query_data.projection_behavior if query_data is not None else None
        ) or self._projection_behavior
        query._query_timings = self._query_timings
        query._explanations = self._explanations
        query._explanation_token = self._explanation_token
        query._is_intersect = self._is_intersect
        query._default_operator = self._default_operator

        return query

    def aggregate_by(
        self, builder_or_facet: Union[Callable[[FacetBuilder], None], FacetBase]
    ) -> AggregationDocumentQuery[_T]:
        if isinstance(builder_or_facet, FacetBase):
            facet = builder_or_facet
        else:
            ff = FacetBuilder()
            builder_or_facet(ff)
            facet = ff.facet

        self._aggregate_by(facet)

        return AggregationDocumentQuery(self)

    def aggregate_by_facets(self, *facets: FacetBase) -> AggregationDocumentQuery[_T]:
        for facet in facets:
            self.aggregate_by(facet)

        return AggregationDocumentQuery(self)

    def aggregate_using(self, facet_setup_document_id: str) -> AggregationDocumentQuery[_T]:
        self._aggregate_using(facet_setup_document_id)
        return AggregationDocumentQuery(self)

    def highlight(
        self,
        field_name: str,
        fragment_length: int,
        fragment_count: int,
        highlightings_callback: Callable[[Highlightings], None],
        options: Optional[HighlightingOptions] = None,
    ) -> DocumentQuery[_T]:
        self._highlight(field_name, fragment_length, fragment_count, options, highlightings_callback)
        return self

    def spatial(
        self,
        field_name_or_field: Union[str, DynamicSpatialField],
        clause: Callable[[SpatialCriteriaFactory], SpatialCriteria],
    ):
        criteria = clause(SpatialCriteriaFactory.instance())
        self._spatial_with_criteria(field_name_or_field, criteria)
        return self

    def within_radius_of(
        self,
        field_name: str,
        radius: float,
        latitude: float,
        longitude: float,
        radius_units: Optional[SpatialUnits] = None,
        distance_error_pct: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> DocumentQuery[_T]:
        self._within_radius_of(field_name, radius, latitude, longitude, radius_units, distance_error_pct)
        return self

    def relates_to_shape(
        self,
        field_name: str,
        shape_wkt: str,
        relation: SpatialRelation,
        units: Optional[SpatialUnits] = None,
        distance_error_pct: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ):
        self._spatial(field_name, shape_wkt, relation, units, distance_error_pct)
        return self

    def order_by_distance(
        self,
        field_or_field_name: Union[str, DynamicSpatialField],
        latitude: float,
        longitude: float,
        round_factor: Optional[float] = 0.0,
    ) -> DocumentQuery[_T]:
        self._order_by_distance(field_or_field_name, latitude, longitude, round_factor)
        return self

    def order_by_distance_wkt(
        self, field_or_field_name: Union[str, DynamicSpatialField], shape_wkt: str
    ) -> DocumentQuery[_T]:
        self._order_by_distance_wkt(field_or_field_name, shape_wkt)
        return self

    def order_by_distance_descending(
        self,
        field_or_field_name: Union[str, DynamicSpatialField],
        latitude: float,
        longitude: float,
        round_factor: Optional[float] = 0.0,
    ) -> DocumentQuery[_T]:
        self._order_by_distance_descending(field_or_field_name, latitude, longitude, round_factor)
        return self

    def order_by_distance_descending_wkt(
        self, field_or_field_name: Union[str, DynamicSpatialField], shape_wkt: str
    ) -> DocumentQuery[_T]:
        self._order_by_distance_descending_wkt(field_or_field_name, shape_wkt)
        return self

    def more_like_this(
        self, more_like_this_or_builder: Union[MoreLikeThisBase, Callable[[MoreLikeThisBuilder[_T]], None]]
    ) -> DocumentQuery[_T]:
        if isinstance(more_like_this_or_builder, MoreLikeThisBase):
            mlt = self._more_like_this()
            mlt.with_options(more_like_this_or_builder.options)

            if isinstance(more_like_this_or_builder, MoreLikeThisUsingDocument):
                mlt.with_document(more_like_this_or_builder.document_json)

            return self

        f = MoreLikeThisBuilder()
        more_like_this_or_builder(f)

        more_like_this = self._more_like_this()
        more_like_this.with_options(f.more_like_this.options)

        if isinstance(f.more_like_this, MoreLikeThisUsingDocument):
            more_like_this_using_document: MoreLikeThisUsingDocument = f.more_like_this
            more_like_this.with_document(more_like_this_using_document.document_json)
        elif isinstance(f.more_like_this, MoreLikeThisUsingDocumentForDocumentQuery):
            more_like_this_using_document_fdq: MoreLikeThisUsingDocumentForDocumentQuery = f.more_like_this
            more_like_this_using_document_fdq.for_document_query(self)
        return self

    def suggest_using(
        self, suggestion_or_builder: Union[SuggestionBase, Callable[[SuggestionBuilder[_T]], None]]
    ) -> SuggestionDocumentQuery[_T]:
        if not isinstance(suggestion_or_builder, SuggestionBase):
            f = SuggestionBuilder()
            suggestion_or_builder(f)
            suggestion_or_builder = f.suggestion

        self._suggest_using(suggestion_or_builder)
        return SuggestionDocumentQuery(self)


class RawDocumentQuery(Generic[_T], AbstractDocumentQuery[_T]):
    def __init__(self, object_type: Type[_T], session: InMemoryDocumentSessionOperations, raw_query: str):
        super().__init__(object_type, session, None, None, False, None, None)
        self._query_raw = raw_query

    def skip(self, count: int) -> RawDocumentQuery[_T]:
        self._skip(count)
        return self

    def take(self, count: int) -> RawDocumentQuery[_T]:
        self._take(count)
        return self

    def wait_for_non_stale_results(self, wait_timeout: Optional[datetime.timedelta] = None):
        self._wait_for_non_stale_results(wait_timeout)
        return self

    def timings(self, timings_callback: Callable[[QueryTimings], None]) -> RawDocumentQuery[_T]:
        self._include_timings(timings_callback)
        return self

    def no_tracking(self) -> RawDocumentQuery[_T]:
        self._no_tracking()
        return self

    def no_caching(self) -> RawDocumentQuery[_T]:
        self._no_caching()
        return self

    def using_default_operator(self, query_operator: QueryOperator) -> RawDocumentQuery[_T]:
        self._using_default_operator(query_operator)
        return self

    def statistics(self, stats_callback: Callable[[QueryStatistics], None]) -> RawDocumentQuery[_T]:
        self._statistics(stats_callback)
        return self

    def add_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> RawDocumentQuery[_T]:
        self._add_after_query_executed_listener(action)
        return self

    def remove_after_query_executed_listener(self, action: Callable[[QueryResult], None]) -> RawDocumentQuery[_T]:
        self._remove_after_query_executed_listener(action)
        return self

    def add_before_query_executed_listener(self, action: Callable[[QueryResult], None]) -> RawDocumentQuery[_T]:
        self._add_before_query_executed_listener(action)
        return self

    def remove_before_query_executed_listener(self, action: Callable[[QueryResult], None]) -> RawDocumentQuery[_T]:
        self._remove_before_query_executed_listener(action)
        return self

    def add_after_stream_executed_listener(self, action: Callable[[dict], None]) -> RawDocumentQuery[_T]:
        self._add_after_stream_executed_listener(action)
        return self

    def remove_after_stream_executed_listener(self, action: Callable[[dict], None]) -> RawDocumentQuery[_T]:
        self._remove_after_stream_executed_listener(action)
        return self

    def add_parameter(self, name: str, value: object) -> RawDocumentQuery[_T]:
        self._add_parameter(name, value)
        return self

    def execute_aggregation(self):
        query = AggregationRawDocumentQuery(self, self._the_session)
        return query.execute()

    def projection(self, projection_behavior: ProjectionBehavior) -> RawDocumentQuery[_T]:
        self._projection(projection_behavior)
        return self


class DocumentQueryCustomizationDelegate(DocumentQueryCustomization):
    def __init__(self, query: AbstractDocumentQuery):
        super().__init__(query)

    def add_before_query_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._before_query_executed_callback.append(action)
        return self

    def remove_before_query_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._before_query_executed_callback.remove(action)
        return self

    def add_after_query_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._after_query_executed_callback.append(action)
        return self

    def remove_after_query_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._after_query_executed_callback.remove(action)
        return self

    def add_after_stream_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._after_stream_executed_callback.append(action)
        return self

    def remove_after_stream_executed_listener(
        self, action: Callable[[IndexQuery], None]
    ) -> DocumentQueryCustomizationDelegate:
        self.query._after_stream_executed_callback.remove(action)
        return self

    def no_caching(self) -> DocumentQueryCustomizationDelegate:
        self.query._no_caching()
        return self

    def no_tracking(self) -> DocumentQueryCustomizationDelegate:
        self.query._no_tracking()
        return self

    def timings(self) -> DocumentQueryCustomizationDelegate:
        self.query._query_timings = self.query._include_timings()
        return self

    def random_ordering(self, seed: Optional[str] = None) -> DocumentQueryCustomizationDelegate:
        self.query._random_ordering(seed)
        return self

    def wait_for_non_stale_results(
        self, wait_timeout: Optional[datetime.timedelta] = None
    ) -> DocumentQueryCustomizationDelegate:
        self.query._wait_for_non_stale_results(wait_timeout)
        return self

    def projection(self, projection_behavior) -> DocumentQueryCustomizationDelegate:
        self.query._projection(projection_behavior)
        return self


class WhereParams:
    def __init__(self):
        self.nested_path = False
        self.allow_wildcards = False

        self.field_name: Union[None, str] = None
        self.value: Union[None, object] = None
        self.exact: Union[None, bool] = None


class QueryStatistics:
    def __init__(
        self,
        is_stale: bool = None,
        duration_in_ms: float = None,
        total_results: int = None,
        skipped_results: int = None,
        timestamp: datetime.datetime = None,
        index_name: str = None,
        index_timestamp: datetime.datetime = None,
        last_query_time: datetime.datetime = None,
        result_etag: int = None,
        node_tag: str = None,
    ):
        self.is_stale = is_stale
        self.duration_in_ms = duration_in_ms
        self.total_results = total_results
        self.skipped_results = skipped_results
        self.timestamp = timestamp
        self.index_name = index_name
        self.index_timestamp = index_timestamp
        self.last_query_time = last_query_time
        self.result_etag = result_etag
        self.node_tag = node_tag

    def update_query_stats(self, qr: QueryResult) -> None:
        self.is_stale = qr.is_stale
        self.duration_in_ms = qr.duration_in_ms
        self.total_results = qr.total_results
        self.skipped_results = qr.skipped_results
        self.timestamp = qr.index_timestamp
        self.index_name = qr.index_name
        self.index_timestamp = qr.index_timestamp
        self.last_query_time = qr.last_query_time
        self.result_etag = qr.result_etag
        self.node_tag = qr.node_tag
