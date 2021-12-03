from typing import Optional

from pyravendb.documents.operations.connection_strings import ConnectionString
from pyravendb.documents.operations.etl import EtlConfiguration
import pyravendb.serverwide


class SqlConnectionString(ConnectionString):
    def __init__(self, name: str, connection_string: Optional[str] = None, factory_name: Optional[str] = None):
        super().__init__(name)
        self.connection_string = connection_string
        self.factory_name = factory_name

    @property
    def get_type(self):
        return pyravendb.serverwide.ConnectionStringType.SQL


# todo: implement
class SqlEtlConfiguration(EtlConfiguration[SqlConnectionString]):
    pass
