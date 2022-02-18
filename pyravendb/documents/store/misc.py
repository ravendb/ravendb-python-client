from __future__ import annotations
import threading
from typing import Generic, Callable, Any, Union, TypeVar

from pyravendb.documents.commands.batches import CommandType

_T = TypeVar("_T")


class Lazy(Generic[_T]):
    def __init__(self, value_factory: Callable[[], Any]):
        self.__value_created = False  # todo: check if it isn't cached - volatile in java
        self.__value_factory = value_factory
        self.__value: Union[None, _T] = None
        self.__value_lock = threading.Lock()

    @property
    def is_value_created(self) -> bool:
        return self.__value_created

    def value(self) -> _T:  # double check locking
        if self.__value_created:
            return self.__value
        with self.__value_lock:
            if not self.__value_created:
                self.__value = self.__value_factory()
                self.__value_created = True
        return self.__value


class IdTypeAndName:
    def __init__(self, key: str, command_type: CommandType, name: str):
        self.key = key
        self.command_type = command_type
        self.name = name

    def __eq__(self, other):
        if other is None or type(self) != type(other):
            return False
        other: IdTypeAndName
        if self.key != other.key if self.key else other.key:
            return False
        if self.command_type != other.command_type:
            return False
        return self.name == other.name if self.name is not None else other.name is None

    def __hash__(self):
        return hash((self.key, self.command_type, self.name))

    @staticmethod
    def create(key: str, command_type: CommandType, name: Union[None, str]) -> IdTypeAndName:
        return IdTypeAndName(key, command_type, name)
