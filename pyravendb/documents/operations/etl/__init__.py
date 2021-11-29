from typing import Optional, Generic, TypeVar, List

from pyravendb.documents.operations.connection_strings import ConnectionString
from pyravendb.serverwide import ConnectionStringType

_T = TypeVar("_T")


class RavenConnectionString(ConnectionString):
    def __init__(self, name: str, database: Optional[str] = None, topology_discovery_urls: Optional[List[str]] = None):
        super().__init__(name)
        self.database = database
        self.topology_discovery_urls = topology_discovery_urls

    @property
    def get_type(self):
        return ConnectionStringType.RAVEN


# todo: implement
class EtlConfiguration(ConnectionString, Generic[_T]):
    pass


class RavenEtlConfiguration(EtlConfiguration[RavenConnectionString]):
    pass
