from __future__ import annotations

import datetime
from abc import abstractmethod
from enum import Enum
from typing import Dict
from typing import TYPE_CHECKING
from ravendb.http.topology import ClusterTopology, NodeStatus

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class ClusterTopologyResponse:
    def __init__(self, leader: str, node_tag: str, topology: ClusterTopology, etag: int, status: Dict[str, NodeStatus]):
        self.leader = leader
        self.node_tag = node_tag
        self.topology = topology
        self.status = status
        self.etag = etag

    @classmethod
    def from_json(cls, json_dict: dict) -> ClusterTopologyResponse:
        return cls(
            json_dict["Leader"],
            json_dict["NodeTag"],
            ClusterTopology.from_json(json_dict["Topology"]),
            json_dict["Etag"],
            json_dict["Status"],
        )


class ReadBalanceBehavior(Enum):
    NONE = "None"
    ROUND_ROBIN = "RoundRobin"
    FASTEST_NODE = "FastestNode"

    def __str__(self):
        return self.value


class LoadBalanceBehavior(Enum):
    NONE = "None"
    USE_SESSION_CONTEXT = "UseSessionContext"

    def __str__(self):
        return self.value


class ResponseDisposeHandling(Enum):
    MANUALLY = "Manually"
    AUTOMATIC = "Automatic"

    def __str__(self):
        return self.value


class AggressiveCacheMode(Enum):
    TRACK_CHANGES = "TrackChanges"
    DO_NOT_TRACK_CHANGES = "DoNotTrackChanges"


class AggressiveCacheOptions:
    def __init__(self, duration: datetime.timedelta, mode: AggressiveCacheMode):
        self.duration = duration
        self.mode = mode


class Broadcast:
    @abstractmethod
    def prepare_to_broadcast(self, conventions: DocumentConventions):
        pass
