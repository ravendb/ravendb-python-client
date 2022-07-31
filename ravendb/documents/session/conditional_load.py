from __future__ import annotations
from typing import Generic, TypeVar, Optional

_T = TypeVar("_T")


class ConditionalLoadResult(Generic[_T]):
    def __init__(self):
        self._entity = None
        self._change_vector = None

    @property
    def entity(self) -> Optional[_T]:
        return self._entity

    @property
    def change_vector(self) -> Optional[str]:
        return self._change_vector

    @classmethod
    def create(cls, entity: Optional[_T], change_vector: Optional[str]) -> ConditionalLoadResult[Optional[_T]]:
        result = cls()
        result._entity = entity
        result._change_vector = change_vector
        return result
