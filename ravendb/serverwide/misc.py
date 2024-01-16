from __future__ import annotations

import datetime
import enum
from typing import List, Optional, Dict, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ravendb.serverwide.operations.common import DatabasePromotionStatus


class DocumentsCompressionConfiguration:
    def __init__(
        self,
        compress_revisions: Optional[bool] = None,
        compress_all_collections: Optional[bool] = None,
        collections: Optional[List[str]] = None,
    ):
        self.compress_revisions = compress_revisions
        self.compress_all_collections = compress_all_collections
        self.collections = collections

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> DocumentsCompressionConfiguration:
        return cls(json_dict["CompressRevisions"], json_dict["CompressAllCollections"], json_dict["Collections"])

    def to_json(self) -> Dict[str, Any]:
        return {
            "CompressRevisions": self.compress_revisions,
            "CompressAllCollections": self.compress_all_collections,
            "Collections": self.collections,
        }


class DeletionInProgressStatus(enum.Enum):
    NO = "NO"
    SOFT_DELETE = "SOFT_DELETE"
    HARD_DELETE = "HARD_DELETE"

    def __str__(self):
        return self.value


class ConflictSolver:
    def __init__(
        self,
        resolve_by_collection: Optional[Dict[str, ScriptResolver]] = None,
        resolve_to_latest: Optional[bool] = None,
    ):
        self.resolve_by_collection = resolve_by_collection
        self.resolve_to_latest = resolve_to_latest


class ScriptResolver:
    def __init__(self, script: Optional[str] = None, last_modified_time: Optional[datetime.datetime] = None):
        self.script = script
        self.last_modified_time = last_modified_time


class DatabaseTopology:
    def __init__(
        self,
        members: Optional[List[str]] = None,
        promotables: Optional[List[str]] = None,
        rehabs: Optional[List[str]] = None,
        predefined_mentors: Optional[Dict[str, str]] = None,
        demotion_reasons: Optional[Dict[str, str]] = None,
        promotables_status: Optional[Dict[str, "DatabasePromotionStatus"]] = None,
        replication_factor: Optional[int] = None,
        dynamic_nodes_distribution: Optional[bool] = None,
        stamp: Optional[LeaderStamp] = None,
        database_topology_id_base_64: Optional[str] = None,
        priority_order: Optional[List[str]] = None,
        nodes_modified_at: Optional[datetime.datetime] = None,
    ):
        self.members = members
        self.promotables = promotables
        self.rehabs = rehabs
        self.predefined_mentors = predefined_mentors
        self.demotion_reasons = demotion_reasons
        self.promotables_status = promotables_status
        self.replication_factor = replication_factor
        self.dynamic_nodes_distribution = dynamic_nodes_distribution
        self.stamp = stamp
        self.database_topology_id_base_64 = database_topology_id_base_64
        self.priority_order = priority_order
        self.nodes_modified_at = nodes_modified_at

    @property
    def all_nodes(self) -> List[str]:
        return [*self.members, *self.promotables, *self.rehabs]

    @classmethod
    def from_json(cls, json_dict: dict) -> DatabaseTopology:
        return cls(
            json_dict["Members"],
            json_dict["Promotables"],
            json_dict["Rehabs"],
            json_dict.get("PredefinedMentors", None),
            json_dict["DemotionReasons"],
            json_dict["PromotablesStatus"],
            json_dict["ReplicationFactor"],
            json_dict["DynamicNodesDistribution"],
            LeaderStamp.from_json(json_dict["Stamp"]),
            json_dict["DatabaseTopologyIdBase64"],
            json_dict["PriorityOrder"],
        )


class LeaderStamp:
    def __init__(self, index: Optional[int] = None, term: Optional[int] = None, leaders_ticks: Optional[int] = None):
        self.index = index
        self.term = term
        self.leaders_ticks = leaders_ticks

    @classmethod
    def from_json(cls, json_dict: Dict) -> LeaderStamp:
        return cls(json_dict["Index"], json_dict["Term"], json_dict["LeadersTicks"])


class CompactSettings:
    def __init__(self, database_name: str = None, documents: bool = None, indexes: List[str] = None):
        self.database_name = database_name
        self.documents = documents
        self.indexes = indexes

    def to_json(self) -> Dict[str, Any]:
        return {"DatabaseName": self.database_name, "Documents": self.documents, "Indexes": self.indexes}
