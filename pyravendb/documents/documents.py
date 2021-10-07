from __future__ import annotations
from pyravendb.documents.commands.batches import CommandType


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
