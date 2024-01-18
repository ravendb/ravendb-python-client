from typing import Optional, Any, Dict


class AnalyzerDefinition:
    def __init__(self, name: Optional[str] = None, code: Optional[str] = None):
        self.name = name
        self.code = code

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AnalyzerDefinition":
        return cls(json_dict["Name"], json_dict["Code"])

    def to_json(self) -> Dict[str, Any]:
        return {"Name": self.name, "Code": self.code}
