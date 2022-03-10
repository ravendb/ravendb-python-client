from typing import TypeVar, Generic

_T = TypeVar("_T")


class Reference(Generic[_T]):
    def __init__(self, value: _T = None):
        self.__value = [value]

    @property
    def value(self) -> _T:
        return self.__value[0]

    @value.setter
    def value(self, value: _T):
        self.__value[0] = value
