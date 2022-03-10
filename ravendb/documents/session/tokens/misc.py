from enum import Enum


class WhereOperator(Enum):
    EQUALS = "Equals"
    NOT_EQUALS = "NotEquals"
    GREATER_THAN = "GreaterThan"
    GREATER_THAN_OR_EQUAL = "GreaterThanOrEqual"
    LESS_THAN = "LessThan"
    LESS_THAN_OR_EQUAL = "LessThanOrEqual"
    IN = "In"
    ALL_IN = "AllIn"
    BETWEEN = "Between"
    SEARCH = "Search"
    LUCENE = "Lucene"
    STARTS_WITH = "StartsWith"
    ENDS_WITH = "EndsWith"
    EXISTS = "Exists"
    SPATIAL_WITHIN = "SpatialWithin"
    SPATIAL_CONTAINS = "SpatialContains"
    SPATIAL_DISJOINT = "SpatialDisjoints"
    SPATIAL_INTERSECTS = "SpatialIntersects"
    REGEX = "Regex"
