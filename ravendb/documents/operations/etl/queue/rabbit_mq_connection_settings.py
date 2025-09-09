from typing import Dict, Any


class RabbitMqConnectionSettings:
    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string

    def to_json(self):
        return {"ConnectionString": self.connection_string}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(connection_string=json_dict.get("ConnectionString"))
