from abc import ABC, abstractmethod
from typing import Union, Set, Dict, Callable, Optional, List, TYPE_CHECKING

from pyravendb.documents.queries.facets.misc import FacetOptions, FacetAggregation
from pyravendb.documents.session.tokens.tokens import FacetToken

if TYPE_CHECKING:
    from pyravendb.documents.queries.facets.builders import RangeBuilder


class FacetAggregationField:
    def __init__(self, name: str = None, display_name: str = None):
        self.name = name
        self.display_name = display_name

    def __eq__(self, o):
        if self == o:
            return True

        if o is None or self.__class__ != type(o):
            return False

        return self.name == o.name and self.display_name == o.display_name

    def __hash__(self):
        return hash((self.name, self.display_name))


class FacetBase(ABC):
    def __init__(self):
        self.display_field_name: Union[None, str] = None
        self.options: Union[None, FacetOptions] = None
        self.aggregations: Dict[FacetAggregation, Set[FacetAggregationField]] = {}

    @abstractmethod
    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        pass


class GenericRangeFacet(FacetBase):
    def __init__(self, parent: Optional[FacetBase] = None):
        super().__init__()
        self.ranges: List[RangeBuilder] = []
        self.__parent = parent

    @staticmethod
    def parse(range_builder: "RangeBuilder", add_query_parameter: Callable[[object], str]) -> str:
        return range_builder.get_string_representation(add_query_parameter)

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        if self.__parent is not None:
            return self.__parent.to_facet_token(add_query_parameter)

        return FacetToken.create(facet__add_query_parameter=(self, add_query_parameter))


class RangeFacet(FacetBase):
    def __init__(self, parent: Optional[FacetBase] = None):
        super().__init__()
        self.ranges: List[str] = []
        self.__parent = parent

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        if self.__parent is not None:
            return self.__parent.to_facet_token(add_query_parameter)

        return FacetToken.create(facet__add_query_parameter=(self, add_query_parameter))


class Facet(FacetBase):
    def __init__(self, field_name: str = None):
        super().__init__()
        self.field_name = field_name

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        return FacetToken.create(facet__add_query_parameter=(self, add_query_parameter))
