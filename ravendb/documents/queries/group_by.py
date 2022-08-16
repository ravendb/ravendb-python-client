from __future__ import annotations
import enum


class GroupByMethod(enum.Enum):
    NONE = "None"
    ARRAY = "Array"


class GroupBy:
    def __init__(self, field: str = None, method: GroupByMethod = None):
        self.field = field
        self.method = method

    @classmethod
    def field(cls, field_name: str) -> GroupBy:
        return cls(field_name, GroupByMethod.NONE)

    @classmethod
    def array(cls, field_name: str) -> GroupBy:
        return cls(field_name, GroupByMethod.ARRAY)
