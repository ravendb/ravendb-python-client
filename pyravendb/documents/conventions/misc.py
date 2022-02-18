from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Tuple

_T = TypeVar("_T")


class ValueForQueryConverter(Generic[_T]):
    @abstractmethod
    def try_to_convert_value_for_query(self, field_name: str, value: _T, for_range: bool) -> Tuple[bool, object]:
        pass
