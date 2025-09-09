from abc import abstractmethod
from typing import Dict, Any


class ConnectionString:
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_type(self):
        pass

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Any:
        pass
