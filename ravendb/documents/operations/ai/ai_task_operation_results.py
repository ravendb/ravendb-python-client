from __future__ import annotations
from typing import Dict, Any, Optional

from ravendb.documents.operations.etl.etl_operation_results import AddEtlOperationResult


class AddAiTaskOperationResult(AddEtlOperationResult):
    """Base result class for AI task add operations."""

    def __init__(
        self,
        raft_command_index: int = 0,
        task_id: int = 0,
        identifier: Optional[str] = None,
    ):
        super().__init__(raft_command_index=raft_command_index, task_id=task_id)
        self.identifier = identifier

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AddAiTaskOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
            identifier=json_dict.get("Identifier"),
        )


class AddGenAiOperationResult(AddAiTaskOperationResult):
    """Result of adding a GenAI task."""

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AddGenAiOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
            identifier=json_dict.get("Identifier"),
        )


class AddEmbeddingsGenerationOperationResult(AddAiTaskOperationResult):
    """Result of adding an Embeddings Generation task."""

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AddEmbeddingsGenerationOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
            identifier=json_dict.get("Identifier"),
        )
