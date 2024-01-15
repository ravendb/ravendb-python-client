import json
from typing import Optional

import requests

from ravendb import RavenCommand, ServerNode
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import OperationIdResult
from ravendb.serverwide.misc import CompactSettings
from ravendb.serverwide.operations.common import ServerOperation


class CompactDatabaseOperation(ServerOperation[OperationIdResult]):
    def __init__(self, compact_settings: CompactSettings) -> None:
        if not compact_settings:
            raise ValueError("Compact settings cannot be None")

        self._compact_settings = compact_settings

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[OperationIdResult]:
        return self.CompactDatabaseCommand(conventions, self._compact_settings)

    class CompactDatabaseCommand(RavenCommand[OperationIdResult]):
        def __init__(self, conventions: DocumentConventions, compact_settings: CompactSettings) -> None:
            super().__init__(OperationIdResult)

            if conventions is None:
                raise ValueError("Conventions cannot be None")

            if compact_settings is None:
                raise ValueError("Compact settings cannot be None")

            self._compact_settings = compact_settings

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/admin/compact"
            request = requests.Request("POST", url)
            request.data = self._compact_settings.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            self.result = OperationIdResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False
