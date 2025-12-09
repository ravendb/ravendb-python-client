from __future__ import annotations
from typing import Dict, Any


class AddEtlOperationResult:
    """Result of adding an ETL task."""

    def __init__(self, raft_command_index: int = 0, task_id: int = 0):
        self.raft_command_index = raft_command_index
        self.task_id = task_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AddEtlOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
        )


class UpdateEtlOperationResult:
    """Result of updating an ETL task."""

    def __init__(self, raft_command_index: int = 0, task_id: int = 0):
        self.raft_command_index = raft_command_index
        self.task_id = task_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "UpdateEtlOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
        )
