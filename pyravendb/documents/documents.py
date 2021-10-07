from __future__ import annotations

from typing import Callable, Any, Union

from pyravendb.documents.commands.batches import CommandType
import asyncio


class Lazy:
    def __init__(self, object_type: type, value_factory: Callable[[], Any]):
        self.__value_created = False  # todo: check if it isn't cached - volatile in java
        self.__value_factory = value_factory
        self.__value: Union[None, object_type] = None

    @property
    def is_value_created(self) -> bool:
        return self.__value_created

    @property
    def value(self) -> object:  # double check locking
        if self.__value_created:
            return self.__value
        with asyncio.Lock():
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
        if self == other:
            return True
        if other is None or type(self) != type(other):
            return False
        other: IdTypeAndName
        if self.key != other.key if self.key else other.key:
            return False
        if self.command_type != other.command_type:
            return False
        return self.name == other.name if self.name is not None else other.name is None

    @staticmethod
    def create(key: str, command_type: CommandType, name: str) -> IdTypeAndName:
        return IdTypeAndName(key, command_type, name)
