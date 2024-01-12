from __future__ import annotations
from typing import Optional, Any, Dict


class SorterDefinition:
    def __init__(self, name: Optional[str] = None, code: Optional[str] = None):
        self.name = name
        self.code = code

    def to_json(self) -> dict:
        return {"Name": self.name, "Code": self.code}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> SorterDefinition:
        return cls(json_dict["Name"], json_dict["Code"])
