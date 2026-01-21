from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar, List, Dict, Any

from ravendb.documents.operations.connection_strings import ConnectionString
from ravendb.documents.operations.etl.etl_type import EtlType
from ravendb.documents.operations.etl.transformation import Transformation
import ravendb.serverwide.server_operation_executor

_T = TypeVar("_T", bound=ConnectionString)


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


class EtlConfiguration(ABC, Generic[_T]):
    """
    Base class for ETL (Extract, Transform, Load) configurations.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        task_id: int = 0,
        connection_string_name: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: bool = False,
        transforms: Optional[List[Transformation]] = None,
        disabled: bool = False,
        allow_etl_on_non_encrypted_channel: bool = False,
    ):
        self._initialized: bool = False
        self._test_mode: bool = False
        self._connection: Optional[_T] = None
        self._transforms: List[Transformation] = transforms if transforms is not None else []

        self.task_id = task_id
        self.name = name
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node
        self.connection_string_name = connection_string_name
        self.disabled = disabled
        self.allow_etl_on_non_encrypted_channel = allow_etl_on_non_encrypted_channel

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def test_mode(self) -> bool:
        return self._test_mode

    @test_mode.setter
    def test_mode(self, value: bool):
        self._test_mode = value

    @property
    def connection(self) -> Optional[_T]:
        return self._connection

    @property
    def transforms(self) -> List[Transformation]:
        return self._transforms

    @transforms.setter
    def transforms(self, value: List[Transformation]):
        self._transforms = value

    def initialize(self, connection_string: _T) -> None:
        """Initialize the configuration with a connection string."""
        if self._initialized:
            return
        self._connection = connection_string
        self._initialized = True

    @abstractmethod
    def get_destination(self) -> str:
        """Returns the destination for this ETL configuration."""
        pass

    @abstractmethod
    def get_default_task_name(self) -> str:
        """Returns the default task name for this configuration."""
        pass

    @property
    @abstractmethod
    def etl_type(self) -> EtlType:
        """Returns the ETL type for this configuration."""
        pass

    @abstractmethod
    def using_encrypted_communication_channel(self) -> bool:
        """Returns True if the connection uses encrypted communication."""
        pass

    def to_json(self) -> Dict[str, Any]:
        return {
            "EtlType": self.etl_type.value if self.etl_type else None,
            "Name": self.name,
            "TaskId": self.task_id,
            "ConnectionStringName": self.connection_string_name,
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
            "AllowEtlOnNonEncryptedChannel": self.allow_etl_on_non_encrypted_channel,
            "Transforms": [t.to_json() for t in self.transforms] if self.transforms else [],
            "Disabled": self.disabled,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EtlConfiguration":
        raise NotImplementedError("Subclasses must implement from_json")


class RavenEtlConfiguration(EtlConfiguration[RavenConnectionString]):
    @property
    def etl_type(self) -> EtlType:
        return EtlType.RAVEN

    def get_destination(self) -> str:
        return self.connection.database if self.connection else ""

    def get_default_task_name(self) -> str:
        return f"Raven ETL to {self.get_destination()}"

    def using_encrypted_communication_channel(self) -> bool:
        return False
