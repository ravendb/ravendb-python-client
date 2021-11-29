from __future__ import annotations
from enum import Enum
from typing import Optional, Union, List


class SpatialFieldType(Enum):
    GEOGRAPHY = "GEOGRAPHY"
    CARTESIAN = "CARTESIAN"

    def __str__(self):
        return self.value


class SpatialSearchStrategy(Enum):
    GEOHASH_PREFIX_TREE = "GEOHASH_PREFIX_TREE"
    QUAD_PREFIX_TREE = "QUAD_PREFIX_TREE"
    BOUNDING_BOX = "BOUNDING_BOX"

    def __str__(self):
        return self.value


class SpatialUnits:
    KILOMETERS = "KILOMETERS"
    MILES = "MILES"


class SpatialOptions:
    DEFAULT_GEOHASH_LEVEL = -9
    DEFAULT_QUAD_TREE_LEVEL = 23

    def __init__(self, options: Optional[SpatialOptions] = None):
        if options is not None:
            self.type = options.type
            self.strategy = options.strategy
            self.max_tree_level = options.max_tree_level
            self.min_x = options.min_x
            self.max_x = options.max_x
            self.min_y = options.min_y
            self.max_y = options.max_y
            self.units = options.units
        else:
            self.type = SpatialFieldType.GEOGRAPHY
            self.strategy = SpatialSearchStrategy.GEOHASH_PREFIX_TREE
            self.max_tree_level = self.DEFAULT_GEOHASH_LEVEL
            self.min_x = -180
            self.max_x = 180
            self.min_y = -90
            self.max_y = 90
            self.units = SpatialUnits.KILOMETERS

    def __eq__(self, other):
        if self == other:
            return True
        if other is None:
            return False
        if type(self) != type(other):
            return False

        other: SpatialOptions

        result = self.type == other.type and self.strategy == other.strategy
        if self.type == SpatialFieldType.GEOGRAPHY:
            result = result and self.units == other.units
        if self.strategy != SpatialSearchStrategy.BOUNDING_BOX:
            result = result and self.max_tree_level == other.max_tree_level
            if self.type == SpatialFieldType.CARTESIAN:
                result = (
                    result
                    and self.min_x == other.min_x
                    and self.max_x == other.max_x
                    and self.min_y == other.min_y
                    and self.max_y == other.max_y
                )
        return result

    # todo: hashCode() override


class AutoSpatialMethodType(Enum):
    POINT = "POINT"
    WKT = "WKT"

    def __str__(self):
        return self.value


class AutoSpatialOptions(SpatialOptions):
    def __init__(self, options: Optional[SpatialOptions] = None):
        super().__init__(options)
        self.method_type: Union[None, AutoSpatialMethodType] = None
        self.method_argument: Union[None, List[str]] = None
