from __future__ import annotations
from abc import abstractmethod
from typing import Generic, Optional, TypeVar, Union, Callable, Dict

from ravendb.documents.queries.facets.definitions import GenericRangeFacet, Facet, FacetBase, FacetAggregationField
from ravendb.documents.queries.facets.misc import FacetOptions, FacetAggregation
from ravendb.tools.utils import CaseInsensitiveSet

_T = TypeVar("_T")


class FacetOperations(Generic[_T]):
    @abstractmethod
    def with_display_name(self, display_name: str) -> FacetOperations[_T]:
        pass

    @abstractmethod
    def with_options(self, options: FacetOptions) -> FacetOperations[_T]:
        pass

    @abstractmethod
    def sum_on(self, path: str, display_name: Optional[str]) -> FacetOperations[_T]:
        pass

    @abstractmethod
    def min_on(self, path: str, display_name: Optional[str]) -> FacetOperations[_T]:
        pass

    @abstractmethod
    def max_on(self, path: str, display_name: Optional[str]) -> FacetOperations[_T]:
        pass

    @abstractmethod
    def average_on(self, path: str, display_name: Optional[str]) -> FacetOperations[_T]:
        pass


class FacetBuilder(Generic[_T], FacetOperations[_T]):
    _rql_keywords = CaseInsensitiveSet({"as", "select", "where", "load", "group", "order", "include", "update"})

    def __init__(self):
        self.__range: Union[None, GenericRangeFacet] = None
        self.__default: Union[None, Facet] = None

    def by_ranges(self, range_: RangeBuilder, *ranges: RangeBuilder) -> FacetOperations[_T]:
        if range_ is None:
            raise ValueError("range cannot be None")

        if self.__range is None:
            self.__range = GenericRangeFacet()

        self.__range.ranges.append(range_)

        if ranges:
            for p in ranges:
                self.__range.ranges.append(p)

        return self

    def by_field(self, field_name: str) -> FacetOperations[_T]:
        if self.__default is None:
            self.__default = Facet()

        if field_name in self._rql_keywords:
            field_name = f"'{field_name}'"

        self.__default.field_name = field_name
        return self

    def all_results(self) -> FacetOperations[_T]:
        if self.__default is None:
            self.__default = Facet()

        self.__default.field_name = None
        return self

    def with_options(self, options: FacetOptions) -> FacetOperations[_T]:
        self.facet.options = options
        return self

    def with_display_name(self, display_name: str) -> FacetOperations[_T]:
        self.facet.display_field_name = display_name
        return self

    def sum_on(self, path: str, display_name: Optional[str] = None) -> FacetOperations[_T]:
        aggregations_map = self.facet.aggregations
        aggregations = aggregations_map[FacetAggregation.SUM] = (
            aggregations_map[FacetAggregation.SUM] if FacetAggregation.SUM in aggregations_map else set()
        )

        aggregation_field = FacetAggregationField()
        aggregation_field.name = path
        aggregation_field.display_name = display_name

        aggregations.add(aggregation_field)

        return self

    def min_on(self, path: str, display_name: Optional[str] = None) -> FacetOperations[_T]:
        aggregations_map = self.facet.aggregations
        aggregations = aggregations_map[FacetAggregation.MIN] = (
            aggregations_map[FacetAggregation.MIN] if FacetAggregation.MIN in aggregations_map else set()
        )

        aggregation_field = FacetAggregationField()
        aggregation_field.name = path
        aggregation_field.display_name = display_name

        aggregations.add(aggregation_field)

        return self

    def max_on(self, path: str, display_name: Optional[str] = None) -> FacetOperations[_T]:
        aggregations_map = self.facet.aggregations
        aggregations = aggregations_map[FacetAggregation.MAX] = (
            aggregations_map[FacetAggregation.MAX] if FacetAggregation.MAX in aggregations_map else set()
        )

        aggregation_field = FacetAggregationField()
        aggregation_field.name = path
        aggregation_field.display_name = display_name

        aggregations.add(aggregation_field)

        return self

    def average_on(self, path: str, display_name: Optional[str] = None) -> FacetOperations[_T]:
        aggregations_map = self.facet.aggregations
        aggregations = aggregations_map[FacetAggregation.AVERAGE] = (
            aggregations_map[FacetAggregation.AVERAGE] if FacetAggregation.AVERAGE in aggregations_map else set()
        )

        aggregation_field = FacetAggregationField()
        aggregation_field.name = path
        aggregation_field.display_name = display_name

        aggregations.add(aggregation_field)

        return self

    @property
    def facet(self) -> FacetBase:
        return self.__default or self.__range


class RangeBuilder(Generic[_T]):
    def __init__(self, path: str):
        self.__less_bound: Union[None, _T] = None
        self.__greater_bound: Union[None, _T] = None
        self.__less_inclusive: Union[None, bool] = None
        self.__greater_inclusive: Union[None, bool] = None

        self.__less_set = False
        self.__greater_set = False

        self.__path = path

    @classmethod
    def from_json(cls, json_dict: Dict) -> RangeBuilder:
        builder = cls(json_dict["Path"])

        builder.__less_bound = json_dict["LessBound"]
        builder.__greater_bound = json_dict["GreaterBound"]
        builder.__less_inclusive = json_dict["LessInclusive"]
        builder.__greater_inclusive = json_dict["GreaterInclusive"]
        builder.__less_set = json_dict["LessSet"]
        builder.__greater_set = json_dict["GreaterSet"]

        return builder

    def to_json(self) -> Dict:
        return {
            "Path": self.__path,
            "LessBound": self.__less_bound,
            "GreaterBound": self.__greater_bound,
            "LessInclusive": self.__less_inclusive,
            "GreaterInclusive": self.__greater_inclusive,
            "LessSet": self.__less_set,
            "GreaterSet": self.__greater_set,
        }

    @classmethod
    def for_path(cls, path: str) -> RangeBuilder[_T]:
        return cls(path)

    def __create_clone(self) -> RangeBuilder[_T]:
        builder = RangeBuilder(self.__path)
        builder.__less_bound = self.__less_bound
        builder.__greater_bound = self.__greater_bound
        builder.__less_inclusive = self.__less_inclusive
        builder.__greater_inclusive = self.__greater_inclusive
        builder.__less_set = self.__less_set
        builder.__greater_set = self.__greater_set
        return builder

    def is_less_than(self, value: _T) -> RangeBuilder[_T]:
        if self.__less_set:
            raise RuntimeError("Less bound was already set")

        clone = self.__create_clone()
        clone.__less_bound = value
        clone.__less_inclusive = False
        clone.__less_set = True
        return clone

    def is_less_than_or_equal_to(self, value: _T) -> RangeBuilder[_T]:
        if self.__less_set:
            raise RuntimeError("Less bound was already set")

        clone = self.__create_clone()
        clone.__less_bound = value
        clone.__less_inclusive = True
        clone.__less_set = True
        return clone

    def is_greater_than(self, value: _T) -> RangeBuilder[_T]:
        if self.__greater_set:
            raise RuntimeError("Greater bound was already set")

        clone = self.__create_clone()
        clone.__greater_bound = value
        clone.__greater_inclusive = False
        clone.__greater_set = True
        return clone

    def is_greater_than_or_equal_to(self, value: _T) -> RangeBuilder[_T]:
        if self.__greater_set:
            raise RuntimeError("Greater bound was already set")

        clone = self.__create_clone()
        clone.__greater_bound = value
        clone.__greater_inclusive = True
        clone.__greater_set = True
        return clone

    def get_string_representation(self, add_query_parameter: Callable[[object], str]):
        less: str = None
        greater: str = None

        if not self.__less_set and not self.__greater_set:
            raise RuntimeError("Bounds were not set")

        if self.__less_set:
            less_param_name = add_query_parameter(self.__less_bound)
            less = self.__path + (" <= " if self.__less_inclusive else " < ") + "$" + less_param_name

        if self.__greater_set:
            greater_param_name = add_query_parameter(self.__greater_bound)
            greater = self.__path + (" >= " if self.__greater_inclusive else " > ") + "$" + greater_param_name

        if less is not None and greater is not None:
            return greater + " and " + less

        return less or greater
