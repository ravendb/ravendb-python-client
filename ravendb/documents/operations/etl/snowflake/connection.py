from typing import Optional, List

from ravendb.documents.operations.connection_strings import ConnectionString, ConnectionStringUsage
import ravendb.serverwide.server_operation_executor


class SnowflakeConnectionString(ConnectionString):
    def __init__(
        self,
        name: str,
        connection_string: Optional[str] = None,
        used_by: Optional[List[ConnectionStringUsage]] = None,
    ):
        super().__init__(name, used_by)
        self.connection_string = connection_string

    @property
    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.SNOWFLAKE.value

    def to_json(self):
        return {
            "Name": self.name,
            "ConnectionString": self.connection_string,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.SNOWFLAKE,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "SnowflakeConnectionString":
        return cls(
            name=json_dict["Name"],
            connection_string=json_dict["ConnectionString"],
            used_by=[ConnectionStringUsage.from_json(usage) for usage in (json_dict.get("UsedBy") or [])],
        )
