from __future__ import annotations
from typing import Any, Dict, Optional


class AiAgentToolSubAgent:
    """Server-side sub-agent the model can call from within a parent agent run."""

    def __init__(self, identifier: Optional[str] = None, description: Optional[str] = None):
        self.identifier = identifier
        self.description = description

    def to_json(self) -> Dict[str, Any]:
        return {
            "Identifier": self.identifier,
            "Description": self.description,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentToolSubAgent:
        return cls(
            identifier=json_dict.get("identifier") or json_dict.get("Identifier"),
            description=json_dict.get("description") or json_dict.get("Description"),
        )
