from __future__ import annotations
from typing import Tuple, Union, List, Optional, Callable

import ravendb.documents.queries.facets.definitions as facets_definitions
from ravendb.documents.queries.facets.misc import FacetAggregation, FacetOptions
from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
from ravendb.tools.utils import Utils, QueryFieldUtil


class FacetToken(QueryToken):
    def __init__(
        self,
        aggregate_by_field_name: Optional[str] = None,
        alias: Optional[str] = None,
        ranges: Optional[List[str]] = None,
        options_parameter_name: Optional[str] = None,
    ):
        self._aggregate_by_field_name = aggregate_by_field_name
        self._alias = alias
        self._ranges = ranges
        self._options_parameter_name = options_parameter_name
        self._aggregations = []
        self._facet_setup_document_id: Optional[str] = None

    @classmethod
    def from_facet_setup_document_id(cls, facet_setup_document_id: str) -> FacetToken:
        token = cls()
        token._facet_setup_document_id = facet_setup_document_id
        return token

    @property
    def name(self) -> str:
        return self._alias or self._aggregate_by_field_name

    @classmethod
    def create(cls) -> FacetToken:
        pass

    @classmethod
    def create_from_facet_setup_document_id(
        cls,
        facet_setup_document_id: str = None,
    ) -> FacetToken:
        if facet_setup_document_id is None or facet_setup_document_id.isspace():
            raise ValueError("facet_setup_document_id cannot be None or whitespace")
        return FacetToken.from_facet_setup_document_id(facet_setup_document_id)

    @classmethod
    def create_from_facet(
        cls, facet: facets_definitions.Facet, add_query_parameter: Callable[[object], str]
    ) -> FacetToken:
        options_parameter_name = cls.__get_options_parameter_name(facet, add_query_parameter)
        token = FacetToken(
            QueryFieldUtil.escape_if_necessary(facet.field_name),
            QueryFieldUtil.escape_if_necessary(facet.display_field_name),
            None,
            options_parameter_name,
        )

        cls.__apply_aggregations(facet, token)

        return token

    @classmethod
    def create_from_range_facet(
        cls, facet: facets_definitions.RangeFacet, add_query_parameter: Callable[[object], str]
    ) -> FacetToken:
        options_parameter_name = cls.__get_options_parameter_name(facet, add_query_parameter)

        token = FacetToken(
            None, QueryFieldUtil.escape_if_necessary(facet.display_field_name), facet.ranges, options_parameter_name
        )

        cls.__apply_aggregations(facet, token)

        return token

    @classmethod
    def create_from_generic_range_facet(
        cls, facet: facets_definitions.GenericRangeFacet, add_query_parameter: Callable[[object], str]
    ) -> FacetToken:
        options_parameter_name = cls.__get_options_parameter_name(facet, add_query_parameter)

        ranges = [
            facets_definitions.GenericRangeFacet.parse(range_builder, add_query_parameter)
            for range_builder in facet.ranges
        ]

        token = FacetToken(
            None, QueryFieldUtil.escape_if_necessary(facet.display_field_name), ranges, options_parameter_name
        )

        cls.__apply_aggregations(facet, token)
        return token

    @classmethod
    def create_from_facet_base(
        cls, facet: facets_definitions.FacetBase, add_query_parameter: Callable[[object], str]
    ) -> FacetToken:
        # this is just a dispatcher
        return facet.to_facet_token(add_query_parameter)

    def write_to(self, writer: List[str]) -> None:
        writer.append("facet(")

        if self._facet_setup_document_id is not None:
            writer.append("id('")
            writer.append(self._facet_setup_document_id)
            writer.append("'))")
            return

        first_argument = False

        if self._aggregate_by_field_name is not None:
            writer.append(self._aggregate_by_field_name)
        elif self._ranges is not None:
            first_in_range = True

            for range_ in self._ranges:
                if not first_in_range:
                    writer.append(", ")

                first_in_range = False
                writer.append(range_)

        else:
            first_argument = True

        for aggregation in self._aggregations:
            if not first_argument:
                writer.append(", ")
            first_argument = False
            aggregation.write_to(writer)

        if self._options_parameter_name and not self._options_parameter_name.isspace():
            writer.append(", $")
            writer.append(self._options_parameter_name)

        writer.append(")")

        if not self._alias or self._alias.isspace() or self._alias == self._aggregate_by_field_name:
            return

        writer.append(" as ")
        writer.append(self._alias)

    @staticmethod
    def __get_options_parameter_name(
        facet: facets_definitions.FacetBase, add_query_parameter: Callable[[object], str]
    ) -> Optional[str]:
        return (
            add_query_parameter(facet.options)
            if facet.options is not None and facet.options != FacetOptions.default_options()
            else None
        )

    @classmethod
    def __apply_aggregations(cls, facet: facets_definitions.FacetBase, token: FacetToken) -> None:
        for key, value in facet.aggregations.items():
            for value_ in value:
                if key == FacetAggregation.MAX:
                    aggregation_token = cls._FacetAggregationToken.max(value_.name, value_.display_name)
                elif key == FacetAggregation.MIN:
                    aggregation_token = cls._FacetAggregationToken.min(value_.name, value_.display_name)
                elif key == FacetAggregation.AVERAGE:
                    aggregation_token = cls._FacetAggregationToken.average(value_.name, value_.display_name)
                elif key == FacetAggregation.SUM:
                    aggregation_token = cls._FacetAggregationToken.sum(value_.name, value_.display_name)
                else:
                    raise NotImplementedError(f"Unsupported aggregation method: {key}")

                token._aggregations.append(aggregation_token)

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

            if not self.__field_display_name or self.__field_display_name.isspace():
                return

            writer.append(" as ")
            self.write_field(writer, self.__field_display_name)

        @classmethod
        def max(cls, field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return cls(field_name, field_display_name, FacetAggregation.MAX)

        @classmethod
        def min(cls, field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return cls(field_name, field_display_name, FacetAggregation.MIN)

        @classmethod
        def average(
            cls, field_name: str, field_display_name: Optional[str] = None
        ) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return cls(field_name, field_display_name, FacetAggregation.AVERAGE)

        @classmethod
        def sum(cls, field_name: str, field_display_name: Optional[str] = None) -> FacetToken._FacetAggregationToken:
            if field_name.isspace():
                raise ValueError("field_name cannot be None")
            return cls(field_name, field_display_name, FacetAggregation.SUM)
