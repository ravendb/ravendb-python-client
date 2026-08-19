from abc import abstractmethod
from typing import Any, Dict, List, Optional


class ConnectionStringUsage:
    """A single usage of a connection string, as reported by the server on GET."""

    def __init__(
        self,
        kind: Optional[str] = None,
        id: Optional[int] = None,
        identifier: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.kind = kind
        self.id = id
        self.identifier = identifier
        self.name = name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ConnectionStringUsage":
        return cls(
            kind=json_dict.get("Kind"),
            id=json_dict.get("Id"),
            identifier=json_dict.get("Identifier"),
            name=json_dict.get("Name"),
        )


class ConnectionString:
    def __init__(self, name: str, used_by: Optional[List[ConnectionStringUsage]] = None):
        self.name = name
        self.used_by = used_by if used_by is not None else []

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
