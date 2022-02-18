from __future__ import annotations
from abc import ABC
from enum import Enum
from typing import List


class MethodCall(ABC):
    def __init__(self, args: List[object] = None, access_path: str = None):
        self.args = args
        self.access_path = access_path


class CmpXchg(MethodCall):
    @staticmethod
    def value(key: str) -> CmpXchg:
        cmp_xchg = CmpXchg()
        cmp_xchg.args = [key]

        return cmp_xchg


class OrderingType(Enum):
    STRING = 0
    LONG = " AS long"
    FLOAT = " AS double"
    ALPHA_NUMERIC = " AS alphaNumeric"

    def __str__(self):
        return self.value
