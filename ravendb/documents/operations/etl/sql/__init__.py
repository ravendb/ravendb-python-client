from typing import Optional

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide
from ravendb.documents.operations.etl.configuration import EtlConfiguration


class SqlConnectionString(ConnectionString):
    def __init__(self, name: str, connection_string: Optional[str] = None, factory_name: Optional[str] = None):
        super().__init__(name)
        self.connection_string = connection_string
        self.factory_name = factory_name

    @property
    def get_type(self):
        return ravendb.serverwide.ConnectionStringType.SQL


# todo: implement
class SqlEtlConfiguration(EtlConfiguration[SqlConnectionString]):
    pass
