from __future__ import annotations

import hashlib
import json
import os
from typing import Optional

from ravendb.http.topology import Topology

# On-disk topology cache (mirrors the .NET client's TopologyLocalCache). The feature is
# opt-in: it is active only when DocumentConventions.topology_cache_location is set.

DATABASE_TOPOLOGY_EXTENSION = ".raven-topology"
CLUSTER_TOPOLOGY_EXTENSION = ".raven-cluster-topology"


def server_hash(url: str, database: Optional[str] = None) -> str:
    key = "{0}{1}".format(url, database if database else "")
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _path(location: str, topology_hash: str, extension: str) -> str:
    return os.path.join(location, topology_hash + extension)


def try_save(location: Optional[str], topology_hash: str, topology: Topology, extension: str) -> None:
    # Best-effort: caching failures must never break a topology update.
    if not location or topology is None:
        return
    try:
        os.makedirs(location, exist_ok=True)
        with open(_path(location, topology_hash, extension), "w", encoding="utf-8") as stream:
            json.dump(topology.to_json(), stream)
    except Exception:
        pass


def try_load(location: Optional[str], topology_hash: str, extension: str) -> Optional[Topology]:
    if not location:
        return None
    try:
        path = _path(location, topology_hash, extension)
        if not os.path.isfile(path):
            return None
        with open(path, "r", encoding="utf-8") as stream:
            return Topology.from_json(json.load(stream))
    except Exception:
        return None
