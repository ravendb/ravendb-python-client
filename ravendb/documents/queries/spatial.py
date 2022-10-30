from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, Optional

from ravendb import constants
from ravendb.documents.indexes.spatial.configuration import SpatialRelation, SpatialUnits
from ravendb.documents.session.tokens.misc import WhereOperator
from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
from ravendb.documents.session.tokens.query_tokens.definitions import ShapeToken, WhereToken


class DynamicSpatialField(ABC):
    def __init__(self, round_factor: float = 0):
        self.__round_factor = round_factor

    @property
    def round_factor(self) -> float:
        return self.__round_factor

    @abstractmethod
    def to_field(self, ensure_valid_field_name: Callable[[str, bool], str]) -> str:
        pass

    def round_to(self, factor: float) -> DynamicSpatialField:
        self.__round_factor = factor
        return self


class PointField(DynamicSpatialField):
    def __init__(self, latitude: str, longitude: str):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude

    def to_field(self, ensure_valid_field_name: Callable[[str, bool], str]) -> str:
        return (
            f"spatial.point({ensure_valid_field_name(self.latitude, False)}, "
            f"{ensure_valid_field_name(self.longitude, False)})"
        )


class WktField(DynamicSpatialField):
    def __init__(self, wkt: str):
        super().__init__()
        self.wkt = wkt

    def to_field(self, ensure_valid_field_name: Callable[[str, bool], str]) -> str:
        return f"spatial.wkt({ensure_valid_field_name(self.wkt, False)})"


class SpatialCriteria(ABC):
    def __init__(self, relation: SpatialRelation, distance_error_pct: float):
        self.__relation = relation
        self.__distance_error_pct = distance_error_pct

    @abstractmethod
    def _get_shape_token(self, add_query_parameter: Callable[[object], str]) -> ShapeToken:
        pass

    def to_query_token(self, field_name: str, add_query_parameter: Callable[[object], str]) -> QueryToken:
        shape_token = self._get_shape_token(add_query_parameter)

        if self.__relation == SpatialRelation.WITHIN:
            where_operator = WhereOperator.SPATIAL_WITHIN
        elif self.__relation == SpatialRelation.CONTAINS:
            where_operator = WhereOperator.SPATIAL_CONTAINS
        elif self.__relation == SpatialRelation.DISJOINT:
            where_operator = WhereOperator.SPATIAL_DISJOINT
        elif self.__relation == SpatialRelation.INTERSECTS:
            where_operator = WhereOperator.SPATIAL_INTERSECTS
        else:
            raise ValueError()

        return WhereToken.create(
            where_operator,
            field_name,
            None,
            WhereToken.WhereOptions(shape__distance=(shape_token, self.__distance_error_pct)),
        )


class WktCriteria(SpatialCriteria):
    def __init__(
        self, shape_wkt: str, relation: SpatialRelation, radius_units: SpatialUnits, distance_error_pct: float
    ):
        super().__init__(relation, distance_error_pct)
        self.__shape_wkt = shape_wkt
        self.__radius_units = radius_units

    def _get_shape_token(self, add_query_parameter: Callable[[object], str]) -> ShapeToken:
        return ShapeToken.wkt(add_query_parameter(self.__shape_wkt), self.__radius_units)


class CircleCriteria(SpatialCriteria):
    def __init__(
        self,
        radius: float,
        latitude: float,
        longitude: float,
        radius_units: SpatialUnits,
        relation: SpatialRelation,
        dist_error_percent: float,
    ):
        super().__init__(relation, dist_error_percent)

        self.__radius = radius
        self.__latitude = latitude
        self.__longitude = longitude
        self.__radius_units = radius_units

    def _get_shape_token(self, add_query_parameter: Callable[[object], str]) -> ShapeToken:
        return ShapeToken.circle(
            add_query_parameter(self.__radius),
            add_query_parameter(self.__latitude),
            add_query_parameter(self.__longitude),
            self.__radius_units,
        )


class SpatialCriteriaFactory:
    INSTANCE = None

    @classmethod
    def instance(cls) -> SpatialCriteriaFactory:
        if cls.INSTANCE is None:
            cls.INSTANCE = SpatialCriteriaFactory()

        return cls.INSTANCE

    def relates_to_shape(
        self,
        shape_wkt: str,
        relation: SpatialRelation,
        units: SpatialUnits = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return WktCriteria(shape_wkt, relation, units, dist_error_percent)

    def intersects(
        self,
        shape_wkt: str,
        units: Optional[SpatialUnits] = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return self.relates_to_shape(shape_wkt, SpatialRelation.INTERSECTS, units, dist_error_percent)

    def contains(
        self,
        shape_wkt: str,
        units: Optional[SpatialUnits] = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return self.relates_to_shape(shape_wkt, SpatialRelation.CONTAINS, units, dist_error_percent)

    def disjoint(
        self,
        shape_wkt: str,
        units: Optional[SpatialUnits] = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return self.relates_to_shape(shape_wkt, SpatialRelation.DISJOINT, units, dist_error_percent)

    def within(
        self,
        shape_wkt: str,
        units: Optional[SpatialUnits] = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return self.relates_to_shape(shape_wkt, SpatialRelation.WITHIN, units, dist_error_percent)

    def within_radius(
        self,
        radius: float,
        latitude: float,
        longitude: float,
        radius_units: Optional[SpatialUnits] = None,
        dist_error_percent: Optional[float] = constants.Documents.Indexing.Spatial.DEFAULT_DISTANCE_ERROR_PCT,
    ) -> SpatialCriteria:
        return CircleCriteria(radius, latitude, longitude, radius_units, SpatialRelation.WITHIN, dist_error_percent)
