from typing import Optional, Generic, TypeVar, List, Dict

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor

_T = TypeVar("_T")


class RavenConnectionString(ConnectionString):
    def __init__(self, name: str, database: Optional[str] = None, topology_discovery_urls: Optional[List[str]] = None):
        super().__init__(name)
        self.database = database
        self.topology_discovery_urls = topology_discovery_urls

    @property
    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.RAVEN.value

    def to_json(self):
        return {
            "Name": self.name,
            "Database": self.database,
            "TopologyDiscoveryUrls": self.topology_discovery_urls,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.RAVEN,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RavenConnectionString":
        return cls(
            name=json_dict["Name"],
            database=json_dict["Database"],
            topology_discovery_urls=json_dict["TopologyDiscoveryUrls"],
        )


# todo: implement
class EtlConfiguration(ConnectionString, Generic[_T]):
    pass


class RavenEtlConfiguration(EtlConfiguration[RavenConnectionString]):
    pass
