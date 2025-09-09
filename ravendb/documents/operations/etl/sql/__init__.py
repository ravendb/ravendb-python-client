from typing import Optional, Dict, Any

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor
from ravendb.documents.operations.etl.configuration import EtlConfiguration


class SqlConnectionString(ConnectionString):
    def __init__(self, name: str, connection_string: Optional[str] = None, factory_name: Optional[str] = None):
        super().__init__(name)
        self.connection_string = connection_string
        self.factory_name = factory_name

    @property
    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.SQL.value

    def to_json(self):
        return {
            "Name": self.name,
            "ConnectionString": self.connection_string,
            "FactoryName": self.factory_name,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.SQL,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "SqlConnectionString":
        return cls(
            name=json_dict["Name"],
            connection_string=json_dict["ConnectionString"],
            factory_name=json_dict["FactoryName"],
        )


# todo: implement
class SqlEtlConfiguration(EtlConfiguration[SqlConnectionString]):
    pass
