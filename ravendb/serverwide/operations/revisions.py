from __future__ import annotations

import json
from typing import Any, Dict, Optional, TYPE_CHECKING

import requests

from ravendb.documents.operations.revisions import RevisionsCollectionConfiguration
from ravendb.http.raven_command import RavenCommand
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.common import ServerOperation
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.http.server_node import ServerNode


class ConfigureRevisionsForConflictsResult:
    def __init__(self, raft_command_index: Optional[int] = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConfigureRevisionsForConflictsResult:
        return cls(json_dict["RaftCommandIndex"])


class ConfigureRevisionsForConflictsOperation(ServerOperation[ConfigureRevisionsForConflictsResult]):
    """Sets the revisions configuration that is applied to conflicting documents of a database."""

    def __init__(self, database: str, configuration: RevisionsCollectionConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")
        self._database = database
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ConfigureRevisionsForConflictsResult]:
        return self.ConfigureRevisionsForConflictsCommand(self._database, self._configuration)

    class ConfigureRevisionsForConflictsCommand(RavenCommand[ConfigureRevisionsForConflictsResult], RaftCommand):
        def __init__(self, database: str, configuration: RevisionsCollectionConfiguration):
            super().__init__(ConfigureRevisionsForConflictsResult)
            if database is None:
                raise ValueError("Database cannot be None")
            self._database = database
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: "ServerNode") -> requests.Request:
            url = f"{node.url}/databases/{self._database}/admin/revisions/conflicts/config"
            request = requests.Request("POST", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ConfigureRevisionsForConflictsResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator().new_id()
