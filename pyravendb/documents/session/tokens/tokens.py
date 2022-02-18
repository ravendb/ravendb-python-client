from __future__ import annotations

import enum
import os
from typing import List, Union, Optional, Tuple, Callable

from pyravendb import constants
import pyravendb.documents.queries.facets.facet as facets
from pyravendb.documents.queries.facets.misc import FacetOptions, FacetAggregation
from pyravendb.documents.session.misc import OrderingType
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.documents.indexes.spatial import SpatialUnits
from pyravendb.documents.queries.group_by import GroupByMethod
from pyravendb.documents.queries.misc import SearchOperator
from pyravendb.documents.queries.query import QueryOperator
from pyravendb.documents.session.tokens.misc import WhereOperator
from pyravendb.documents.session.tokens.query_token import QueryToken
from pyravendb.documents.session.utils.document_query import DocumentQueryHelper
from pyravendb.tools.utils import Utils


class CompareExchangeValueIncludesToken(QueryToken):
    def __init__(self, path: str):
        if not path:
            raise ValueError("Path cannot be None")
        self.__path = path

    @staticmethod
    def create(path: str):
        return CompareExchangeValueIncludesToken(path)

    def write_to(self, writer: List[str]):
        writer.append(f"cmpxchg('{self.__path}')")


class CounterIncludesToken(QueryToken):
    def __init__(self, source_path: str, counter_name: Union[None, str], is_all: bool):
        self.__counter_name = counter_name
        self.__all = is_all
        self.__source_path = source_path

    @staticmethod
    def create(source_path: str, counter_name: str):
        return CounterIncludesToken(source_path, counter_name, False)

    @staticmethod
    def all(source_path: str):
        return CounterIncludesToken(source_path, None, True)

    def add_alias_to_path(self, alias: str):
        self.__source_path = alias if not self.__source_path else f"{alias}.{self.__source_path}"

    def write_to(self, writer: List[str]):
        writer.append("counters(")
        if self.__source_path:
            writer.append(self.__source_path)

            if not self.__all:
                writer.append(", ")

        if not self.__all:
            writer.append(f"'{self.__counter_name}'")

        writer.append(")")


class TimeSeriesIncludesToken(QueryToken):
    def __init__(self, source_path: str, time_range: TimeSeriesRange):
        self.__range = time_range
        self.__source_path = source_path

    @staticmethod
    def create(source_path: str, time_range: TimeSeriesRange):
        return TimeSeriesIncludesToken(source_path, time_range)

    def add_alias_to_path(self, alias: str):
        self.__source_path = alias if not self.__source_path else f"{alias}.{self.__source_path}"

    def write_to(self, writer: List[str]):
        writer.append("timeseries(")
        if self.__source_path:
            writer.append(f"{self.__source_path}, ")

        writer.append(f"'{self.__range.name}', ")
        writer.append(f"'{Utils.datetime_to_string(self.__range.from_date)}', " if self.__range.from_date else "null,")
        writer.append(f"'{Utils.datetime_to_string(self.__range.to_date)}', " if self.__range.to_date else "null")
        writer.append(")")


class FieldsToFetchToken(QueryToken):
    def __init__(
        self, fields_to_fetch: List[str], projections: Union[List[str], None], custom_function: bool, source_alias: str
    ):
        self.fields_to_fetch = fields_to_fetch
        self.projections = projections
        self.custom_function = custom_function
        self.source_alias = source_alias

    @staticmethod
    def create(fields_to_fetch: List[str], projections: List[str], custom_function: bool, source_alias=None):
        if not fields_to_fetch:
            raise ValueError("fields_to_fetch cannot be None")
        if (not custom_function) and projections is not None and len(projections) != len(fields_to_fetch):
            raise ValueError("Length of projections must be the same as length of field to fetch")

        return FieldsToFetchToken(fields_to_fetch, projections, custom_function, source_alias)

    def write_to(self, writer: List[str]):
        for i in range(len(self.fields_to_fetch)):
            field_to_fetch = self.fields_to_fetch[i]
            if i > 0:
                writer.append(" ,")
            if not field_to_fetch:
                writer.append("null")
            else:
                super().write_field(writer, field_to_fetch)

            if self.custom_function:
                continue

            projection = self.projections[i] if self.projections else None
            if projection or projection == field_to_fetch:
                continue

            writer.append(" as ")
            writer.append(projection)


class DeclareToken(QueryToken):
    def __init__(self, name: str, body: str, parameters: str, time_series: bool):
        self.__name = name
        self.__body = body
        self.__parameters = parameters
        self.__time_series = time_series

    @staticmethod
    def create_function(name: str, body: str, parameters: Optional[str] = None) -> DeclareToken:
        return DeclareToken(name, body, parameters, False)

    @staticmethod
    def create_time_series(name: str, body: str, parameters: Optional[str] = None) -> DeclareToken:
        return DeclareToken(name, body, parameters, True)

    def write_to(self, writer: List[str]) -> None:
        writer.append("declare ")
        writer.append("timeseries " if {self.__time_series} else "function ")
        writer.append(self.__name)
        writer.append("(")
        writer.append(self.__parameters)
        writer.append(") ")
        writer.append("{")
        writer.append(os.linesep)
        writer.append(self.__body)
        writer.append(os.linesep)
        writer.append("}")
        writer.append(os.linesep)


class LoadToken(QueryToken):
    def __init__(self, argument: str, alias: str):
        self.argument = argument
        self.alias = alias

    @staticmethod
    def create(argument: str, alias: str) -> LoadToken:
        return LoadToken(argument, alias)

    def write_to(self, writer: List[str]) -> None:
        writer.append(self.argument)
        writer.append(" as ")
        writer.append(self.alias)


class FromToken(QueryToken):
    __WHITE_SPACE_CHARS = [" ", "\t", "\r", "\n"]

    def __init__(self, index_name: str, collection_name: str, alias: Optional[str] = None):
        self.__index_name = index_name
        self.__collection_name = collection_name
        self.__dynamic = collection_name is not None
        self.__alias = alias

    @property
    def collection_name(self) -> str:
        return self.__collection_name

    @property
    def index_name(self) -> str:
        return self.__index_name

    @property
    def dynamic(self) -> bool:
        return self.__dynamic

    @property
    def alias(self) -> str:
        return self.__alias

    @staticmethod
    def create(index_name: str, collection_name: str, alias: str) -> FromToken:
        return FromToken(index_name, collection_name, alias)

    def write_to(self, writer: List[str]) -> None:
        if self.__index_name is None and self.__collection_name is None:
            raise ValueError("Either index_name or collection_name must be specified")

        if self.__dynamic:
            writer.append("from '")
            writer.append(Utils.quote_key(self.__collection_name, reserved_at=True))
            writer.append("'")
        else:
            writer.append("from index '")
            writer.append(self.__index_name)
            writer.append("'")

        if self.__alias is not None:
            writer.append(" as ")
            writer.append(self.__alias)


class OrderByToken(QueryToken):
    @staticmethod
    def random():
        return OrderByToken("random()", False, OrderingType.STRING)

    @staticmethod
    def score_ascending():
        return OrderByToken("score()", False, OrderingType.STRING)

    @staticmethod
    def score_descending():
        return OrderByToken("score()", True, OrderingType.STRING)

    def __init__(self, field_name: str, descending: bool, ordering_or_sorter_name: Union[OrderingType, str]):
        self.__field_name = field_name
        self.__descending = descending
        is_ordering = isinstance(ordering_or_sorter_name, OrderingType)
        self.__ordering = ordering_or_sorter_name if is_ordering else None
        self.__sorter_name = None if is_ordering else ordering_or_sorter_name

    @staticmethod
    def create_distance_ascending(
        field_name: str,
        latitude_and_longitude_or_shape_wkt_parameter_names: Union[Tuple[str, str], str],
        round_factor_parameter_name: str,
    ) -> OrderByToken:
        if isinstance(latitude_and_longitude_or_shape_wkt_parameter_names, tuple):
            latitude_parameter_name, longitude_parameter_name = latitude_and_longitude_or_shape_wkt_parameter_names
            return OrderByToken(
                f"spatial.distance({field_name}), "
                f"spatial.point(${latitude_parameter_name}, "
                f"${longitude_parameter_name})"
                f"{'' if round_factor_parameter_name is None else ', $'+round_factor_parameter_name+')'}",
                False,
                OrderingType.STRING,
            )
        elif isinstance(latitude_and_longitude_or_shape_wkt_parameter_names, str):
            return OrderByToken(
                f"spatial.distance({field_name}), "
                f"spatial.wkt(${latitude_and_longitude_or_shape_wkt_parameter_names})"
                f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name + ')'}",
                False,
                OrderingType.STRING,
            )
        raise TypeError("Pass either string or two strings in the tuple (shape_wkt or (latitude, longitude))")

    @staticmethod
    def create_distance_descending(
        field_name: str,
        latitude_and_longitude_or_shape_wkt_parameter_names: Union[Tuple[str, str], str],
        round_factor_parameter_name: str,
    ) -> OrderByToken:
        if isinstance(latitude_and_longitude_or_shape_wkt_parameter_names, tuple):
            latitude_parameter_name, longitude_parameter_name = latitude_and_longitude_or_shape_wkt_parameter_names
            return OrderByToken(
                f"spatial.distance({field_name}), "
                f"spatial.point(${latitude_parameter_name}, "
                f"${longitude_parameter_name})"
                f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name + ')'}",
                True,
                OrderingType.STRING,
            )
        elif isinstance(latitude_and_longitude_or_shape_wkt_parameter_names, str):
            return OrderByToken(
                f"spatial.distance({field_name}), spatial.wkt(${latitude_and_longitude_or_shape_wkt_parameter_names})"
                f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name + ')'}",
                True,
                OrderingType.STRING,
            )
        raise TypeError("Pass either string or two strings in the tuple (shape_wkt or (latitude, longitude))")

    @staticmethod
    def create_random(seed: str) -> OrderByToken:
        if seed is None:
            raise ValueError("Seed cannot be None")

        return OrderByToken("random('" + seed.replace("'", "''") + "')", False, OrderingType.STRING)

    @staticmethod
    def create_ascending(field_name: str, sorter_name_or_ordering_type: Union[OrderingType, str]) -> OrderByToken:
        return OrderByToken(field_name, False, sorter_name_or_ordering_type)

    @staticmethod
    def create_descending(field_name: str, sorter_name_or_ordering_type: Union[OrderingType, str]) -> OrderByToken:
        return OrderByToken(field_name, True, sorter_name_or_ordering_type)

    def write_to(self, writer: List[str]) -> None:
        if self.__sorter_name is not None:
            writer.append("custom(")

        self.write_field(writer, self.__field_name)

        if self.__sorter_name is not None:
            writer.append(", '")
            writer.append(self.__sorter_name)
            writer.append("')")
        else:
            if self.__ordering == OrderingType.LONG:
                writer.append(" as long")
            elif self.__ordering == OrderingType.FLOAT:
                writer.append(" as double")
            elif self.__ordering == OrderingType.ALPHA_NUMERIC:
                writer.append(" as alphaNumeric")

        if self.__descending:
            writer.append(" desc")


class GroupByToken(QueryToken):
    def __init__(self, field_name: str, method: GroupByMethod):
        self.__field_name = field_name
        self.__method = method

    @staticmethod
    def create(field_name: str, method: Optional[GroupByMethod] = GroupByMethod.NONE) -> GroupByToken:
        return GroupByToken(field_name, method)

    def write_to(self, writer: List[str]):
        if self.__method is not GroupByMethod.NONE:
            writer.append("Array(")
        self.write_field(writer, self.__field_name)
        if self.__method is not GroupByMethod.NONE:
            writer.append(")")


class GroupByKeyToken(QueryToken):
    def __init__(self, field_name: str, projected_name: str):
        self.__field_name = field_name
        self.__projected_name = projected_name

    @staticmethod
    def create(field_name: str, projected_name: str) -> GroupByKeyToken:
        return GroupByKeyToken(field_name, projected_name)

    def write_to(self, writer: List[str]):
        self.write_field(writer, self.__field_name or "key()")
        if self.__projected_name is None or self.__projected_name == self.__field_name:
            return

        writer.append(" as ")
        writer.append(self.__projected_name)


class GroupBySumToken(QueryToken):
    def __init__(self, field_name: str, projected_name: str):
        if field_name is None:
            raise ValueError("field_name cannot be None")

        self.__field_name = field_name
        self.__projected_name = projected_name

    @staticmethod
    def create(field_name: str, projected_name: str) -> GroupBySumToken:
        return GroupBySumToken(field_name, projected_name)

    def write_to(self, writer: List[str]):
        writer.append("sum(")
        writer.append(self.__field_name)
        writer.append(")")

        if self.__projected_name is None:
            return

        writer.append(" as ")
        writer.append(self.__projected_name)


class GroupByCountToken(QueryToken):
    def __init__(self, field_name: str):
        self.__field_name = field_name

    @staticmethod
    def create(field_name: str) -> GroupByCountToken:
        return GroupByCountToken(field_name)

    def write_to(self, writer: List[str]):
        writer.append("count()")

        if self.__field_name is None:
            return

        writer.append(" as ")
        writer.append(self.__field_name)


class MoreLikeThisToken(QueryToken):
    def __init__(self):
        self.where_tokens: List[QueryToken] = []
        self.document_parameter_name: Union[None, str] = None
        self.options_parameter_name: Union[None, str] = None

    def write_to(self, writer: List[str]):
        writer.append("moreLikeThis(")
        if self.document_parameter_name is None:
            for i in range(len(self.where_tokens)):
                DocumentQueryHelper.add_space_if_needed(
                    self.where_tokens[i - 1] if i > 0 else None, self.where_tokens[i], writer
                )
                self.where_tokens[i].write_to(writer)
        else:
            writer.append("$")
            writer.append(self.document_parameter_name)

        if self.options_parameter_name is None:
            writer.append(")")
            return

        writer.append(", $")
        writer.append(self.options_parameter_name)
        writer.append(")")


class OpenSubclauseToken(QueryToken):
    def __init__(self):
        self.boost_parameter_name: Union[None, str] = None

    @staticmethod
    def create() -> OpenSubclauseToken:
        return OpenSubclauseToken()

    def write_to(self, writer: List[str]):
        if self.boost_parameter_name is not None:
            writer.append("boost")

        writer.append("(")


class CloseSubclauseToken(QueryToken):
    def __init__(self):
        self.boost_parameter_name: Union[None, str] = None

    @staticmethod
    def create() -> CloseSubclauseToken:
        return CloseSubclauseToken()

    def write_to(self, writer: List[str]):
        if self.boost_parameter_name is not None:
            writer.append(", $")
            writer.append(self.boost_parameter_name)

        writer.append(")")


class IntersectMarkerToken(QueryToken):
    __INSTANCE = None

    @classmethod
    def instance(cls) -> IntersectMarkerToken:
        if cls.__INSTANCE is None:
            cls.__INSTANCE = IntersectMarkerToken()
        return cls.__INSTANCE

    def write_to(self, writer: List[str]):
        writer.append(",")


class ShapeToken(QueryToken):
    def __init__(self, shape: str):
        self.__shape = shape

    @staticmethod
    def circle(
        radius_parameter_name: str,
        latitude_parameter_name: str,
        longitude_parameter_name: str,
        radius_units: SpatialUnits,
    ):
        if radius_units is None:
            return ShapeToken(
                f"spatial.circle(${radius_parameter_name}, "
                f"${latitude_parameter_name}, "
                f"${longitude_parameter_name})"
            )

        if radius_units == SpatialUnits.KILOMETERS:
            return ShapeToken(
                f"spatial.circle(${radius_parameter_name}, "
                f"${latitude_parameter_name}, "
                f"${longitude_parameter_name}, "
                f"'Kilometers')"
            )

        return ShapeToken(
            f"spatial.circle(${radius_parameter_name}, "
            f"${latitude_parameter_name}, "
            f"${longitude_parameter_name}, "
            f"'Miles')"
        )

    @staticmethod
    def wkt(shape_wkt_parameter_name: str, units: SpatialUnits) -> ShapeToken:
        if units is None:
            return ShapeToken(f"spatial.wkt(${shape_wkt_parameter_name})")

        if units == SpatialUnits.KILOMETERS:
            return ShapeToken(f"spatial.wkt(${shape_wkt_parameter_name}, " f"'Kilometers')")

        return ShapeToken(f"spatial.wkt(${shape_wkt_parameter_name}, " f"'Miles')")

    def write_to(self, writer: List[str]):
        writer.append(self.__shape)


class WhereToken(QueryToken):
    class MethodsType(enum.Enum):
        CMP_X_CHG = "CmpXChg"

    class WhereMethodCall:
        def __init__(
            self, method_type: WhereToken.MethodsType = None, parameters: List[str] = None, property: str = None
        ):
            self.method_type = method_type
            self.parameters = parameters
            self.property = property

    class WhereOptions:
        def __init__(
            self,
            search: Optional[SearchOperator] = None,
            shape__distance: Optional[Tuple[ShapeToken, float]] = None,
            exact__from__to: Optional[Tuple[bool, Optional[str], Optional[str]]] = None,
            method_type__parameters__property__exact: Optional[
                Tuple[WhereToken.MethodsType, List[str], str, Optional[bool]]
            ] = None,
        ):
            args_count = len(
                list(
                    filter(
                        lambda x: x is not None,
                        [search, shape__distance, exact__from__to, method_type__parameters__property__exact],
                    )
                )
            )

            if args_count > 1:
                raise ValueError(
                    f"WhereOptions init takes only one argument at max. {args_count} arguments were given."
                )

            self.search_operator = None
            self.from_parameter_name = None
            self.to_parameter_name = None
            self.boost = None
            self.fuzzy = None
            self.proximity = None
            self.exact = None
            self.method = None
            self.where_shape = None
            self.distance_error_pct = None

            if search is not None:
                self.search_operator = search

            elif shape__distance is not None:
                self.where_shape = shape__distance[0]
                self.distance_error_pct = shape__distance[1]

            elif exact__from__to is not None:
                self.exact = exact__from__to[0]
                self.from_parameter_name = exact__from__to[1]
                self.to_parameter_name = exact__from__to[2]

            elif method_type__parameters__property__exact is not None:
                self.method = WhereToken.WhereMethodCall(
                    method_type__parameters__property__exact[0],
                    method_type__parameters__property__exact[1],
                    method_type__parameters__property__exact[2],
                )
                self.exact = method_type__parameters__property__exact[3]

        @staticmethod
        def default_options() -> WhereToken.WhereOptions:
            return WhereToken.WhereOptions()

    def __init__(
        self,
        field_name: str,
        where_operator: WhereOperator,
        parameter_name: str,
        where_options: Optional[WhereToken.WhereOptions] = None,
    ):
        self.field_name = field_name
        self.where_operator = where_operator
        self.parameter_name = parameter_name
        self.options = where_options

    @staticmethod
    def create(
        op: WhereOperator,
        field_name: str,
        parameter_name: Union[None, str],
        options: Optional[WhereToken.WhereOptions] = None,
    ) -> WhereToken:
        return WhereToken(field_name, op, parameter_name, options or WhereToken.WhereOptions.default_options())

    def add_alias(self, alias: str) -> WhereToken:
        if self.field_name == "id()":
            return self

        return WhereToken(f"{alias}.{self.field_name}", self.where_operator, self.parameter_name, self.options)

    def __write_method(self, writer: List[str]) -> bool:
        if self.options.method is not None:
            if self.options.method.method_type == WhereToken.MethodsType.CMP_X_CHG:
                writer.append("cmpxchg(")
            else:
                raise ValueError(f"Unsupported method: {self.options.method.method_type}")

            first = True
            for parameter in self.options.method.parameters:
                if not first:
                    writer.append(",")
                first = False
                writer.append("$")
                writer.append(parameter)
            writer.append(")")

            if self.options.method.property is not None:
                writer.append(".")
                writer.append(self.options.method.property)

            return True
        return False

    def write_to(self, writer: List[str]) -> None:
        if self.options.boost is not None:
            writer.append("boost(")

        if self.options.fuzzy is not None:
            writer.append("fuzzy(")

        if self.options.proximity is not None:
            writer.append("proximity(")

        if self.options.exact:
            writer.append("exact(")

        if self.where_operator == WhereOperator.SEARCH:
            writer.append("search(")
        elif self.where_operator == WhereOperator.LUCENE:
            writer.append("lucene(")
        elif self.where_operator == WhereOperator.STARTS_WITH:
            writer.append("startsWith(")
        elif self.where_operator == WhereOperator.ENDS_WITH:
            writer.append("endsWith(")
        elif self.where_operator == WhereOperator.EXISTS:
            writer.append("exists(")
        elif self.where_operator == WhereOperator.SPATIAL_WITHIN:
            writer.append("spatial.within(")
        elif self.where_operator == WhereOperator.SPATIAL_CONTAINS:
            writer.append("spatial.within(")
        elif self.where_operator == WhereOperator.SPATIAL_DISJOINT:
            writer.append("spatial.disjoint(")
        elif self.where_operator == WhereOperator.SPATIAL_INTERSECTS:
            writer.append("spatial.intersects(")
        elif self.where_operator == WhereOperator.REGEX:
            writer.append("regex(")

        self.__write_inner_where(writer)

        if self.options.exact:
            writer.append(")")

        if self.options.proximity is not None:
            writer.append(", ")
            writer.append(str(self.options.proximity))
            writer.append(")")
        if self.options.fuzzy is not None:
            writer.append(", ")
            writer.append(str(self.options.fuzzy))
            writer.append(")")

        if self.options.boost is not None:
            writer.append(", ")
            writer.append(str(self.options.boost))
            writer.append(")")

    def __write_inner_where(self, writer: List[str]) -> None:
        self.write_field(writer, self.field_name)

        if self.where_operator == WhereOperator.EQUALS:
            writer.append(" = ")
        elif self.where_operator == WhereOperator.NOT_EQUALS:
            writer.append(" != ")
        elif self.where_operator == WhereOperator.GREATER_THAN:
            writer.append(" > ")
        elif self.where_operator == WhereOperator.GREATER_THAN_OR_EQUAL:
            writer.append(" >= ")
        elif self.where_operator == WhereOperator.LESS_THAN:
            writer.append(" < ")
        elif self.where_operator == WhereOperator.LESS_THAN_OR_EQUAL:
            writer.append(" <= ")
        else:
            self.__special_operator(writer)
            return

        if not self.__write_method(writer):
            writer.append("$")
            writer.append(self.parameter_name)

    def __special_operator(self, writer: List[str]):
        if self.where_operator == WhereOperator.IN:
            writer.append(" in ($")
            writer.append(self.parameter_name)
            writer.append(")")
        elif self.where_operator == WhereOperator.ALL_IN:
            writer.append(" all in ($")
            writer.append(self.parameter_name)
            writer.append(")")
        elif self.where_operator == WhereOperator.BETWEEN:
            writer.append(" between $")
            writer.append(self.options.from_parameter_name)
            writer.append(" and $")
            writer.append(self.options.to_parameter_name)
        elif self.where_operator == WhereOperator.SEARCH:
            writer.append(", $")
            writer.append(self.parameter_name)
            if self.options.search_operator == SearchOperator.AND:
                writer.append(", and")
            writer.append(")")
        elif self.where_operator in [
            WhereOperator.LUCENE,
            WhereOperator.STARTS_WITH,
            WhereOperator.ENDS_WITH,
            WhereOperator.REGEX,
        ]:
            writer.append(", $")
            writer.append(self.parameter_name)
            writer.append(")")
        elif self.where_operator == WhereOperator.EXISTS:
            writer.append(")")
        elif self.where_operator in [
            WhereOperator.SPATIAL_WITHIN,
            WhereOperator.SPATIAL_CONTAINS,
            WhereOperator.SPATIAL_DISJOINT,
            WhereOperator.SPATIAL_INTERSECTS,
        ]:
            writer.append(", ")
            self.options.where_shape.write_to(writer)

            if (
                abs(self.options.distance_error_pct - constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT)
                > 1e-40
            ):
                writer.append(", ")
                writer.append(str(self.options.distance_error_pct))

        else:
            raise TypeError("Unexpected WhereToken.where_operator")


class TrueToken(QueryToken):
    __INSTANCE = None

    @classmethod
    def instance(cls) -> TrueToken:
        if cls.__INSTANCE is None:
            cls.__INSTANCE = TrueToken()
        return cls.__INSTANCE

    def write_to(self, writer: List[str]):
        writer.append("true")


class NegateToken(QueryToken):
    __INSTANCE = None

    @classmethod
    def instance(cls) -> NegateToken:
        if cls.__INSTANCE is None:
            cls.__INSTANCE = NegateToken()
        return cls.__INSTANCE

    def write_to(self, writer: List[str]):
        writer.append("not")


class QueryOperatorToken(QueryToken):
    def __init__(self, query_operator: QueryOperator):
        self.__query_operator = query_operator

    @staticmethod
    def AND() -> QueryOperatorToken:
        return QueryOperatorToken(QueryOperator.AND)

    @staticmethod
    def OR() -> QueryOperatorToken:
        return QueryOperatorToken(QueryOperator.OR)

    def write_to(self, writer: List[str]):
        if self.__query_operator == QueryOperator.AND:
            writer.append("and")
            return

        writer.append("or")


class HighlightingToken(QueryToken):
    def __init__(self, field_name: str, fragment_length: int, fragment_count: int, operations_parameter_name: str):
        self.__field_name = field_name
        self.__fragment_length = fragment_length
        self.__fragment_count = fragment_count
        self.__operations_parameter_name = operations_parameter_name

    @staticmethod
    def create(field_name: str, fragment_length: int, fragment_count: int, operations_parameter_name: str):
        return HighlightingToken(field_name, fragment_length, fragment_count, operations_parameter_name)

    def write_to(self, writer: List[str]):
        writer.append("highlight(")
        self.write_field(writer, self.__field_name)

        writer.append(",")
        writer.append(str(self.__fragment_length))
        writer.append(",")
        writer.append(str(self.__fragment_count))

        if self.__operations_parameter_name is not None:
            writer.append(",$")
            writer.append(self.__operations_parameter_name)

        writer.append(")")


class ExplanationToken(QueryToken):
    def __init__(self, options_parameter_name: str):
        self.__options_parameter_name = options_parameter_name

    @staticmethod
    def create(options_parameter_name: str) -> ExplanationToken:
        return ExplanationToken(options_parameter_name)

    def write_to(self, writer: List[str]) -> None:
        writer.append("explanations(")
        if self.__options_parameter_name is not None:
            writer.append("$")
            writer.append(self.__options_parameter_name)

        writer.append(")")


class TimingsToken(QueryToken):
    __INSTANCE = None

    @classmethod
    def instance(cls) -> TimingsToken:
        if cls.__INSTANCE is None:
            cls.__INSTANCE = TimingsToken()
        return cls.__INSTANCE

    def write_to(self, writer: List[str]):
        writer.append("timings()")


class DistinctToken(QueryToken):
    __INSTANCE = None

    @classmethod
    def instance(cls) -> DistinctToken:
        if cls.__INSTANCE is None:
            cls.__INSTANCE = DistinctToken()
        return cls.__INSTANCE

    def write_to(self, writer: List[str]):
        writer.append("distinct")


class SuggestToken(QueryToken):
    def __init__(
        self,
        field_name: str,
        alias: Union[None, str],
        term_parameter_name: str,
        options_parameter_name: Union[None, str],
    ):
        if field_name is None:
            raise ValueError("field_name cannot be None")

        if term_parameter_name is None:
            raise ValueError("term_parameter_name cannot be None")

        self.__field_name = field_name
        self.__alias = alias
        self.__term_parameter_name = term_parameter_name
        self.__options_parameter_name = options_parameter_name

    @staticmethod
    def create(
        field_name: str, alias: Union[None, str], term_parameter_name: str, options_parameter_name: Union[None, str]
    ):
        return SuggestToken(field_name, alias, term_parameter_name, options_parameter_name)

    @property
    def field_name(self) -> str:
        return self.__field_name

    def write_to(self, writer: List[str]) -> None:
        writer.append("suggest(")
        writer.append(self.field_name)
        writer.append(", $")
        writer.append(self.__term_parameter_name)

        if self.__options_parameter_name is not None:
            writer.append(", $")
            writer.append(self.__options_parameter_name)

        writer.append(")")

        if self.__alias.isspace() or self.field_name == self.__alias:
            return

        writer.append(" as ")
        writer.append(self.__alias)


class FacetToken(QueryToken):
    def __init__(
        self,
        aggregate_by_field_name__alias__ranges__options_parameter_name: Optional[
            Tuple[Union[None, str], str, Union[None, List[str]], str]
        ] = None,
        facet_setup_document_id: Optional[str] = None,
    ):

        self.__aggregations: Union[None, List[FacetToken._FacetAggregationToken]] = None
        if aggregate_by_field_name__alias__ranges__options_parameter_name is not None:
            self.__aggregate_by_field_name = aggregate_by_field_name__alias__ranges__options_parameter_name[0]
            self.__alias = aggregate_by_field_name__alias__ranges__options_parameter_name[1]
            self.__ranges = aggregate_by_field_name__alias__ranges__options_parameter_name[2]
            self.__options_parameter_name = aggregate_by_field_name__alias__ranges__options_parameter_name[3]
            self.__facet_setup_document_id = None
            self.__aggregations = []
            return

        if facet_setup_document_id is not None:
            self.__facet_setup_document_id = facet_setup_document_id
            self.__aggregate_by_field_name = None
            self.__alias = None
            self.__ranges = None
            self.__options_parameter_name = None
            return

        raise ValueError(
            "Pass either (aggregate_by_field_name, alias, ranges, options_parameter_name) tuple or "
            "facet_setup_document_id"
        )

    @property
    def name(self) -> str:
        return self.__alias or self.__aggregate_by_field_name

    @staticmethod
    def create(
        facet_setup_document_id: Optional[str] = None,
        facet__add_query_parameter: Optional[
            Tuple[
                Union[facets.Facet, facets.RangeFacet, facets.GenericRangeFacet, facets.FacetBase],
                Callable[[object], str],
            ]
        ] = None,
    ):
        if facet_setup_document_id is not None:
            if facet_setup_document_id.isspace():
                raise ValueError("facet_setup_document_id cannot be None")

            return FacetToken(facet_setup_document_id=facet_setup_document_id)

        elif facet__add_query_parameter:
            facet = facet__add_query_parameter[0]
            add_query_parameter = facet__add_query_parameter[1]
            ranges = None

            if isinstance(facet, facets.FacetBase):
                return facet.to_facet_token(add_query_parameter)

            options_parameter_name = FacetToken.__get_options_parameter_name(facet, add_query_parameter)
            if isinstance(facet, facets.GenericRangeFacet):
                ranges = []
                for range_builder in facet.ranges:
                    ranges.append(facets.GenericRangeFacet.parse(range_builder, add_query_parameter))

            if isinstance(facet, facets.Facet):
                token = FacetToken(
                    aggregate_by_field_name__alias__ranges__options_parameter_name=(
                        Utils.quote_key(facet.field_name),
                        Utils.quote_key(facet.display_field_name),
                        None,
                        options_parameter_name,
                    )
                )
            else:
                token = FacetToken(
                    aggregate_by_field_name__alias__ranges__options_parameter_name=(
                        None,
                        Utils.quote_key(facet.display_field_name),
                        ranges if isinstance(facet, facets.GenericRangeFacet) else facet.ranges,
                        options_parameter_name,
                    )
                )
            FacetToken.__apply_aggregations(facet, token)
            return token

    @staticmethod
    def __apply_aggregations(facet: facets.FacetBase, token: FacetToken) -> None:
        for key, value in facet.aggregations.items():
            for value_ in value:
                if key == FacetAggregation.MAX:
                    aggregation_token = FacetToken._FacetAggregationToken.max(value_.name, value_.display_name)
                elif key == FacetAggregation.MIN:
                    aggregation_token = FacetToken._FacetAggregationToken.min(value_.name, value_.display_name)
                elif key == FacetAggregation.AVERAGE:
                    aggregation_token = FacetToken._FacetAggregationToken.average(value_.name, value_.display_name)
                elif key == FacetAggregation.SUM:
                    aggregation_token = FacetToken._FacetAggregationToken.sum(value_.name, value_.display_name)
                else:
                    raise NotImplementedError(f"Unsupported aggregation method: {key}")

                token.__aggregations.append(aggregation_token)

    @staticmethod
    def __get_options_parameter_name(
        facet: facets.FacetBase, add_query_parameter: Callable[[object], str]
    ) -> Union[None, str]:
        return (
            add_query_parameter(facet.options)
            if facet.options is not None and facet.options != FacetOptions.default_options()
            else None
        )

    class _FacetAggregationToken(QueryToken):
        def __init__(self, field_name: str, field_display_name: str, aggregation: FacetAggregation):
            self.__field_name = field_name
            self.__field_display_name = field_display_name
            self.__aggregation = aggregation

        def write_to(self, writer: List[str]) -> None:
            if self.__aggregation == FacetAggregation.MAX:
                writer.append("max(")
                writer.append(self.__field_name)
                writer.append(")")

            elif self.__aggregation == FacetAggregation.MIN:
                writer.append("min(")
                writer.append(self.__field_name)
                writer.append(")")

            elif self.__aggregation == FacetAggregation.AVERAGE:
                writer.append("avg(")
                writer.append(self.__field_name)
                writer.append(")")

            elif self.__aggregation == FacetAggregation.SUM:
                writer.append("sum(")
                writer.append(self.__field_name)
                writer.append(")")

            else:
                raise ValueError(f"Invalid aggregation mode: {self.__aggregation}")

            if self.__field_display_name.isspace():
                return

            writer.append(" as ")
            self.write_field(writer, self.__field_display_name)

        @staticmethod
        def max(field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return FacetToken._FacetAggregationToken(field_name, field_display_name, FacetAggregation.MAX)

        @staticmethod
        def min(field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return FacetToken._FacetAggregationToken(field_name, field_display_name, FacetAggregation.MIN)

        @staticmethod
        def average(field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return FacetToken._FacetAggregationToken(field_name, field_display_name, FacetAggregation.AVERAGE)

        @staticmethod
        def sum(field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return FacetToken._FacetAggregationToken(field_name, field_display_name, FacetAggregation.SUM)
