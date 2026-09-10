import enum
from abc import abstractmethod
from typing import Any, Dict, List, Optional


class ConnectionStringUsageKind(enum.Enum):
    """The kind of task or agent that references a connection string."""

    RAVEN_ETL = "RavenEtl"
    SQL_ETL = "SqlEtl"
    OLAP_ETL = "OlapEtl"
    ELASTIC_SEARCH_ETL = "ElasticSearchEtl"
    QUEUE_ETL = "QueueEtl"
    SNOWFLAKE_ETL = "SnowflakeEtl"
    QUEUE_SINK = "QueueSink"
    EXTERNAL_REPLICATION = "ExternalReplication"
    PULL_REPLICATION_AS_SINK = "PullReplicationAsSink"
    EMBEDDINGS_GENERATION = "EmbeddingsGeneration"
    GEN_AI = "GenAi"
    AI_AGENT = "AiAgent"
    CDC_SINK = "CdcSink"

    def __str__(self):
        return self.value


class ConnectionStringUsage:
    """
    One task or agent that references a connection string. Computed server-side and
    returned when reading connection strings; it is never sent back on a write.
    """

    def __init__(
        self,
        kind: ConnectionStringUsageKind = None,
        id_: Optional[int] = None,
        identifier: str = None,
        name: str = None,
    ):
        self.kind = kind
        # The numeric task id, for ongoing tasks (ETL, replication, sinks). None for AI agents.
        self.id_ = id_
        # The string identifier, for AI agents. None for ongoing tasks.
        self.identifier = identifier
        self.name = name

    def to_json(self) -> Dict[str, Any]:
        return {
            "Kind": self.kind.value if self.kind else None,
            "Id": self.id_,
            "Identifier": self.identifier,
            "Name": self.name,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ConnectionStringUsage":
        kind = json_dict.get("Kind")
        return cls(
            kind=ConnectionStringUsageKind(kind) if kind else None,
            id_=json_dict.get("Id"),
            identifier=json_dict.get("Identifier"),
            name=json_dict.get("Name"),
        )

    @classmethod
    def list_from_json(cls, json_list: Optional[List[Dict[str, Any]]]) -> List["ConnectionStringUsage"]:
        return [cls.from_json(usage) for usage in json_list or []]


class ConnectionString:
    def __init__(self, name: str):
        self.name = name
        # Populated when reading connection strings back from the server. Left empty on
        # anything the client builds, and never written by to_json.
        self.used_by: List[ConnectionStringUsage] = []

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
