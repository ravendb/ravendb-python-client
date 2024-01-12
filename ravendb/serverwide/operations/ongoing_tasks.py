from abc import ABC
from typing import List, Any, Dict


class IServerWideTask(ABC):
    def __init__(self, excluded_databases: List[str] = None):
        self.excluded_databases = excluded_databases


class ServerWideTaskResponse:
    def __init__(self, name: str = None, raft_command_index: int = None):
        self.name = name
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ServerWideTaskResponse":
        return cls(json_dict["Name"], json_dict["RaftCommandIndex"])
