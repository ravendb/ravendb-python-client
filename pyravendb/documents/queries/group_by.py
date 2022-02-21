from __future__ import annotations
import enum


class GroupByMethod(enum.Enum):
    NONE = "None"
    ARRAY = "Array"


class GroupBy:
    def __init__(self, field: str = None, method: GroupByMethod = None):
        self.field = field
        self.method = method

    @staticmethod
    def field(field_name: str) -> GroupBy:
        return GroupBy(field_name, GroupByMethod.NONE)

    @staticmethod
    def array(field_name: str) -> GroupBy:
        return GroupBy(field_name, GroupByMethod.ARRAY)
