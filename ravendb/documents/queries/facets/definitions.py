from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Union, Set, Dict, Callable, Optional, List, TYPE_CHECKING

from ravendb.documents.queries.facets.misc import FacetOptions, FacetAggregation
from ravendb.documents.session.tokens.query_tokens.facets import FacetToken

if TYPE_CHECKING:
    from ravendb.documents.queries.facets.builders import RangeBuilder


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

    def to_json(self) -> Dict:
        return {"Name": self.name, "DisplayName": self.display_name}

    @classmethod
    def from_json(cls, json_dict: Dict) -> FacetAggregationField:
        return cls(json_dict["Name"], json_dict["DisplayName"])


class FacetBase(ABC):
    def __init__(self):
        self.display_field_name: Union[None, str] = None
        self.options: Union[None, FacetOptions] = None
        self.aggregations: Dict[FacetAggregation, Set[FacetAggregationField]] = {}

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        pass

    def to_json(self) -> Dict:
        return {
            "DisplayFieldName": self.display_field_name,
            "Options": self.options.to_json(),
            "Aggregations": {
                aggregation.to_json(): {aggregation_field.to_json() for aggregation_field in aggregation_fields_set}
                for aggregation, aggregation_fields_set in self.aggregations.items()
            },
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> FacetBase:
        facet_base = cls()
        facet_base.display_field_name = json_dict["DisplayFieldName"]
        facet_base.options = FacetOptions.from_json(json_dict["Options"])
        facet_base.aggregations = {
            FacetAggregation.from_json(aggregation): {
                FacetAggregationField.from_json(field) for field in aggregation_fields
            }
            for aggregation, aggregation_fields in json_dict["Aggregations"]
        }
        return facet_base


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

        return FacetToken.create_from_generic_range_facet(self, add_query_parameter)


class RangeFacet(FacetBase):
    def __init__(self, parent: Optional[FacetBase] = None):
        super().__init__()
        self.ranges: List[str] = []
        self.__parent = parent

    @classmethod
    def from_json(cls, json_dict: Dict) -> RangeFacet:
        range_facet = cls(FacetBase.from_json(json_dict))
        range_facet.aggregations = range_facet.__parent.aggregations
        range_facet.options = range_facet.__parent.options
        range_facet.display_field_name = range_facet.__parent.display_field_name
        range_facet.ranges = json_dict["Ranges"]
        return range_facet

    def to_json(self) -> Dict:
        super_json = super().to_json()
        super_json.update({"Ranges": self.ranges})
        return super_json

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        if self.__parent is not None:
            return self.__parent.to_facet_token(add_query_parameter)

        return FacetToken.create_from_range_facet(self, add_query_parameter)


class Facet(FacetBase):
    def __init__(self, field_name: str = None):
        super().__init__()
        self.field_name = field_name

    def to_facet_token(self, add_query_parameter: Callable[[object], str]) -> FacetToken:
        return FacetToken.create_from_facet(self, add_query_parameter)

    @classmethod
    def from_json(cls, json_dict: Dict) -> Facet:
        return cls(json_dict["FieldName"])

    def to_json(self) -> Dict:
        super_json = super().to_json()
        super_json.update({"FieldName": self.field_name})
        return super_json


class FacetSetup:
    def __init__(self, facets: List[Facet] = None, range_facets: List[RangeFacet] = None):
        self.facets = facets or []
        self.range_facets = range_facets or []

    @classmethod
    def from_json(cls, json_dict: Dict) -> FacetSetup:
        return cls(
            [Facet.from_json(facet_json) for facet_json in json_dict["Facets"]],
            [RangeFacet.from_json(range_facet_json) for range_facet_json in json_dict["RangeFacets"]],
        )

    def to_json(self) -> Dict:
        return {
            "Facets": [facet.to_json() for facet in self.facets],
            "RangeFacets": [range_facet.to_json() for range_facet in self.range_facets],
        }
