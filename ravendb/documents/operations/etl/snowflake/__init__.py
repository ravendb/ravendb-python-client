from typing import Optional

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor


class SnowflakeConnectionString(ConnectionString):
    def __init__(self, name: str, connection_string: Optional[str] = None):
        super().__init__(name)
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