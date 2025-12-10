from __future__ import annotations
from enum import Enum


class StartingPointChangeVector(str, Enum):
    """
    Represents a starting point for ETL operations based on change vectors.
    """

    DO_NOT_CHANGE = "DoNotChange"
    LAST_DOCUMENT = "LastDocument"
    BEGINNING_OF_TIME = "BeginningOfTime"

    @classmethod
    def from_value(cls, change_vector: str) -> StartingPointChangeVector:
        """Creates a StartingPointChangeVector from a custom change vector value or returns matching enum."""
        # Try to match existing enum values
        for member in cls:
            if member.value == change_vector:
                return member
        # For custom change vectors, return as string (will work since we inherit from str)
        raise ValueError(f"Unknown change vector: {change_vector}. Use one of: {[m.value for m in cls]}")
