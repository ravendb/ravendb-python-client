from __future__ import annotations
import threading
from typing import Generic, Callable, Any, Union, TypeVar

from ravendb.documents.commands.batches import CommandType

_T = TypeVar("_T")


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

    @classmethod
    def create(cls, key: str, command_type: CommandType, name: Union[None, str]) -> IdTypeAndName:
        return cls(key, command_type, name)
