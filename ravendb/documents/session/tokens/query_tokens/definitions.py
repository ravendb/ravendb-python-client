from __future__ import annotations

import enum
import os
from typing import List, Union, Optional, Tuple

from ravendb import constants
from ravendb.documents.session.misc import OrderingType
from ravendb.data.timeseries import TimeSeriesRange
from ravendb.documents.indexes.spatial.configuration import SpatialUnits
from ravendb.documents.queries.group_by import GroupByMethod
from ravendb.documents.queries.misc import SearchOperator
from ravendb.documents.queries.query import QueryOperator
from ravendb.documents.session.tokens.misc import WhereOperator
from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
from ravendb.documents.session.utils.document_query import DocumentQueryHelper
from ravendb.tools.utils import Utils


class CompareExchangeValueIncludesToken(QueryToken):
    def __init__(self, path: str):
        if not path:
            raise ValueError("Path cannot be None")
        self.__path = path

    @classmethod
    def create(cls, path: str):
        return cls(path)

    def write_to(self, writer: List[str]):
        writer.append(f"cmpxchg('{self.__path}')")


class CounterIncludesToken(QueryToken):
    def __init__(self, source_path: str, counter_name: Union[None, str], is_all: bool):
        self.__counter_name = counter_name
        self.__all = is_all
        self.__source_path = source_path

    @classmethod
    def create(cls, source_path: str, counter_name: str) -> CounterIncludesToken:
        return cls(source_path, counter_name, False)

    @classmethod
    def all(cls, source_path: str) -> CounterIncludesToken:
        return cls(source_path, None, True)

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

    @classmethod
    def create(cls, source_path: str, time_range: TimeSeriesRange) -> TimeSeriesIncludesToken:
        return cls(source_path, time_range)

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

    @classmethod
    def create(
        cls, fields_to_fetch: List[str], projections: List[str], custom_function: bool, source_alias=None
    ) -> FieldsToFetchToken:
        if not fields_to_fetch:
            raise ValueError("fields_to_fetch cannot be None")
        if (not custom_function) and projections is not None and len(projections) != len(fields_to_fetch):
            raise ValueError("Length of projections must be the same as length of field to fetch")

        return FieldsToFetchToken(fields_to_fetch, projections, custom_function, source_alias)

    def write_to(self, writer: List[str]):
        for i in range(len(self.fields_to_fetch)):
            field_to_fetch = self.fields_to_fetch[i]
            if i > 0:
                writer.append(", ")
            if not field_to_fetch:
                writer.append("null")
            else:
                super().write_field(writer, field_to_fetch)

            if self.custom_function:
                continue

            projection = self.projections[i] if self.projections else None
            if not projection or projection == field_to_fetch:
                continue

            writer.append(" as ")
            writer.append(projection)


class DeclareToken(QueryToken):
    def __init__(self, name: str, body: str, parameters: str, time_series: bool):
        self.__name = name
        self.__body = body
        self.__parameters = parameters
        self.__time_series = time_series

    @classmethod
    def create_function(cls, name: str, body: str, parameters: Optional[str] = None) -> DeclareToken:
        return cls(name, body, parameters, False)

    @classmethod
    def create_time_series(cls, name: str, body: str, parameters: Optional[str] = None) -> DeclareToken:
        return cls(name, body, parameters, True)

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

    @classmethod
    def create(cls, argument: str, alias: str) -> LoadToken:
        return cls(argument, alias)

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

    @classmethod
    def create(cls, index_name: str, collection_name: str, alias: str) -> FromToken:
        return cls(index_name, collection_name, alias)

    def write_to(self, writer: List[str]) -> None:
        if self.__index_name is None and self.__collection_name is None:
            raise ValueError("Either index_name or collection_name must be specified")

        if self.__dynamic:
            writer.append("from '")
            writer.append(Utils.escape_collection_name(self.__collection_name))
            writer.append("'")
        else:
            writer.append("from index '")
            writer.append(self.__index_name)
            writer.append("'")

        if self.__alias is not None:
            writer.append(" as ")
            writer.append(self.__alias)


class OrderByToken(QueryToken):
    @classmethod
    def random(cls) -> OrderByToken:
        return cls("random()", False, OrderingType.STRING)

    @classmethod
    def score_ascending(cls) -> OrderByToken:
        return cls("score()", False, OrderingType.STRING)

    @classmethod
    def score_descending(cls) -> OrderByToken:
        return cls("score()", True, OrderingType.STRING)

    def __init__(self, field_name: str, descending: bool, ordering_or_sorter_name: Union[OrderingType, str]):
        self.__field_name = field_name
        self.__descending = descending
        is_ordering = isinstance(ordering_or_sorter_name, OrderingType)
        self.__ordering = ordering_or_sorter_name if is_ordering else None
        self.__sorter_name = None if is_ordering else ordering_or_sorter_name

    @classmethod
    def create_distance_ascending_wkt(
        cls, field_name: str, wkt_parameter_name: str, round_factor_parameter_name: str
    ) -> OrderByToken:
        return cls(
            f"spatial.distance({field_name}), "
            f"spatial.wkt(${wkt_parameter_name})"
            f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name})",
            False,
            OrderingType.STRING,
        )

    @classmethod
    def create_distance_ascending(
        cls,
        field_name: str,
        latitude_parameter_name: str,
        longitude_parameter_name: str,
        round_factor_parameter_name: str,
    ) -> OrderByToken:
        return cls(
            f"spatial.distance({field_name}, "
            f"spatial.point(${latitude_parameter_name}, "
            f"${longitude_parameter_name})"
            f"{'' if round_factor_parameter_name is None else ', $'+round_factor_parameter_name})",
            False,
            OrderingType.STRING,
        )

    @classmethod
    def create_distance_descending_wkt(cls, field_name: str, wkt_parameter_name: str, round_factor_parameter_name: str):
        return cls(
            f"spatial.distance({field_name}, "
            f"spatial.wkt(${wkt_parameter_name})"
            f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name})",
            True,
            OrderingType.STRING,
        )

    @classmethod
    def create_distance_descending(
        cls,
        field_name: str,
        latitude_parameter_name: str,
        longitude_parameter_name: str,
        round_factor_parameter_name: str,
    ) -> OrderByToken:
        return cls(
            f"spatial.distance({field_name}, "
            f"spatial.point(${latitude_parameter_name}, "
            f"${longitude_parameter_name})"
            f"{'' if round_factor_parameter_name is None else ', $' + round_factor_parameter_name})",
            True,
            OrderingType.STRING,
        )

    @classmethod
    def create_random(cls, seed: str) -> OrderByToken:
        if seed is None:
            raise ValueError("Seed cannot be None")

        return cls("random('" + seed.replace("'", "''") + "')", False, OrderingType.STRING)

    @classmethod
    def create_ascending(cls, field_name: str, sorter_name_or_ordering_type: Union[OrderingType, str]) -> OrderByToken:
        return cls(field_name, False, sorter_name_or_ordering_type)

    @classmethod
    def create_descending(cls, field_name: str, sorter_name_or_ordering_type: Union[OrderingType, str]) -> OrderByToken:
        return cls(field_name, True, sorter_name_or_ordering_type)

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

    @classmethod
    def create(cls, field_name: str, method: Optional[GroupByMethod] = GroupByMethod.NONE) -> GroupByToken:
        return cls(field_name, method)

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

    @classmethod
    def create(cls, field_name: str, projected_name: str) -> GroupByKeyToken:
        return cls(field_name, projected_name)

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

    @classmethod
    def create(cls, field_name: str, projected_name: str) -> GroupBySumToken:
        return cls(field_name, projected_name)

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

    @classmethod
    def create(cls, field_name: str) -> GroupByCountToken:
        return cls(field_name)

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

    @classmethod
    def create(cls) -> OpenSubclauseToken:
        return cls()

    def write_to(self, writer: List[str]):
        if self.boost_parameter_name is not None:
            writer.append("boost")

        writer.append("(")


class CloseSubclauseToken(QueryToken):
    def __init__(self):
        self.boost_parameter_name: Union[None, str] = None

    @classmethod
    def create(cls) -> CloseSubclauseToken:
        return cls()

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

    @classmethod
    def circle(
        cls,
        radius_parameter_name: str,
        latitude_parameter_name: str,
        longitude_parameter_name: str,
        radius_units: SpatialUnits,
    ) -> ShapeToken:
        if radius_units is None:
            return cls(
                f"spatial.circle(${radius_parameter_name}, "
                f"${latitude_parameter_name}, "
                f"${longitude_parameter_name})"
            )

        if radius_units == SpatialUnits.KILOMETERS:
            return cls(
                f"spatial.circle(${radius_parameter_name}, "
                f"${latitude_parameter_name}, "
                f"${longitude_parameter_name}, "
                f"'Kilometers')"
            )

        return cls(
            f"spatial.circle(${radius_parameter_name}, "
            f"${latitude_parameter_name}, "
            f"${longitude_parameter_name}, "
            f"'Miles')"
        )

    @classmethod
    def wkt(cls, shape_wkt_parameter_name: str, units: SpatialUnits) -> ShapeToken:
        if units is None:
            return cls(f"spatial.wkt(${shape_wkt_parameter_name})")

        if units == SpatialUnits.KILOMETERS:
            return cls(f"spatial.wkt(${shape_wkt_parameter_name}, " f"'Kilometers')")

        return cls(f"spatial.wkt(${shape_wkt_parameter_name}, " f"'Miles')")

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

        @classmethod
        def default_options(cls) -> WhereToken.WhereOptions:
            return cls()

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

    @classmethod
    def create(
        cls,
        op: WhereOperator,
        field_name: str,
        parameter_name: Union[None, str],
        options: Optional[WhereToken.WhereOptions] = None,
    ) -> WhereToken:
        return cls(field_name, op, parameter_name, options or WhereToken.WhereOptions.default_options())

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
                writer.append("..")
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
            writer.append("spatial.contains(")
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
            writer.append(")")

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

    @classmethod
    def AND(cls) -> QueryOperatorToken:
        return cls(QueryOperator.AND)

    @classmethod
    def OR(cls) -> QueryOperatorToken:
        return cls(QueryOperator.OR)

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

    @classmethod
    def create(
        cls, field_name: str, fragment_length: int, fragment_count: int, operations_parameter_name: str
    ) -> HighlightingToken:
        return cls(field_name, fragment_length, fragment_count, operations_parameter_name)

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

    @classmethod
    def create(cls, options_parameter_name: str) -> ExplanationToken:
        return cls(options_parameter_name)

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

    @classmethod
    def create(
        cls,
        field_name: str,
        alias: Union[None, str],
        term_parameter_name: str,
        options_parameter_name: Union[None, str],
    ):
        return cls(field_name, alias, term_parameter_name, options_parameter_name)

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

        if not self.__alias or self.__alias.isspace() or self.field_name == self.__alias:
            return

        writer.append(" as ")
        writer.append(self.__alias)
