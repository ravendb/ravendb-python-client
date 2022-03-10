from __future__ import annotations
from typing import Tuple, Union, List, Optional, Callable

import ravendb.documents.queries.facets.definitions as facets_definitions
from ravendb.documents.queries.facets.misc import FacetAggregation, FacetOptions
from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
from ravendb.tools.utils import Utils


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
            self.__aggregations: List[FacetToken._FacetAggregationToken] = []

        elif facet_setup_document_id is not None:
            self.__facet_setup_document_id = facet_setup_document_id
            self.__aggregate_by_field_name: Optional[str] = None
            self.__alias: Optional[str] = None
            self.__ranges: Optional[List[str]] = None
            self.__options_parameter_name: Optional[str] = None

        else:
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
                Union[
                    facets_definitions.Facet,
                    facets_definitions.RangeFacet,
                    facets_definitions.GenericRangeFacet,
                    facets_definitions.FacetBase,
                ],
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

            if isinstance(facet, facets_definitions.Facet):
                options_parameter_name = FacetToken.__get_options_parameter_name(facet, add_query_parameter)
                token = FacetToken(
                    aggregate_by_field_name__alias__ranges__options_parameter_name=(
                        Utils.quote_key(facet.field_name),
                        Utils.quote_key(facet.display_field_name),
                        None,
                        options_parameter_name,
                    )
                )
            elif isinstance(facet, facets_definitions.RangeFacet):
                options_parameter_name = FacetToken.__get_options_parameter_name(facet, add_query_parameter)
                token = FacetToken(
                    aggregate_by_field_name__alias__ranges__options_parameter_name=(
                        None,
                        Utils.quote_key(facet.display_field_name),
                        ranges if isinstance(facet, facets_definitions.GenericRangeFacet) else facet.ranges,
                        options_parameter_name,
                    )
                )
            elif isinstance(facet, facets_definitions.GenericRangeFacet):
                options_parameter_name = FacetToken.__get_options_parameter_name(facet, add_query_parameter)
                ranges = []
                for range_builder in facet.ranges:
                    ranges.append(facets_definitions.GenericRangeFacet.parse(range_builder, add_query_parameter))
                token = FacetToken(
                    aggregate_by_field_name__alias__ranges__options_parameter_name=(
                        None,
                        Utils.quote_key(facet.display_field_name),
                        ranges,
                        options_parameter_name,
                    )
                )

            elif isinstance(facet, facets_definitions.FacetBase):
                return facet.to_facet_token(add_query_parameter)

            else:
                raise TypeError(f"Unexpected facet type {facet.__class__.__name__}")

            FacetToken.__apply_aggregations(facet, token)
            return token

    @staticmethod
    def __apply_aggregations(facet: facets_definitions.FacetBase, token: FacetToken) -> None:
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
        facet: facets_definitions.FacetBase, add_query_parameter: Callable[[object], str]
    ) -> Union[None, str]:
        return (
            add_query_parameter(facet.options)
            if facet.options is not None and facet.options != FacetOptions.default_options()
            else None
        )

    def write_to(self, writer: List[str]) -> None:
        writer.append("facet(")

        if self.__facet_setup_document_id is not None:
            writer.append("id(")
            writer.append(self.__facet_setup_document_id)
            writer.append("'))")
            return

        first_argument = False

        if self.__aggregate_by_field_name is not None:
            writer.append(self.__aggregate_by_field_name)
        elif self.__ranges is not None:
            first_in_range = True

            for range in self.__ranges:
                if not first_in_range:
                    writer.append(", ")

                first_in_range = False
                writer.append(range)

        else:
            first_argument = True

        for aggregation in self.__aggregations:
            if not first_argument:
                writer.append(", ")
            first_argument = False
            aggregation.write_to(writer)

        if not self.__options_parameter_name.isspace():
            writer.append(", $")
            writer.append(self.__options_parameter_name)

        writer.append(")")

        if not self.__alias or self.__alias.isspace() or self.__alias == self.__aggregate_by_field_name:
            return

        writer.append(" as ")
        writer.append(self.__alias)

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
