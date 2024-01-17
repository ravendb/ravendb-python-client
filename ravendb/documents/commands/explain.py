from __future__ import annotations

import json
from typing import Dict, Any, TYPE_CHECKING, Optional, List

import requests
from ravendb import RavenCommand, IndexQuery, ServerNode
from ravendb.extensions.json_extensions import JsonExtensions

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class ExplainQueryResult:
    def __init__(self, index: str, reason: str):
        self.index = index
        self.reason = reason

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ExplainQueryResult:
        return cls(json_dict["Index"], json_dict["Reason"])


class ExplainQueryCommand(RavenCommand[List[ExplainQueryResult]]):
    def __init__(self, conventions: DocumentConventions, index_query: IndexQuery):
        super().__init__(list)
        if conventions is None:
            raise ValueError("Conventions cannot be None")
        if index_query is None:
            raise ValueError("Index query cannot be None")

        self._conventions = conventions
        self._index_query = index_query

    def create_request(self, node: ServerNode) -> requests.Request:
        path = f"{node.url}/databases/{node.database}/queries?debug=explain"
        return requests.Request(
            "POST", path, data=JsonExtensions.write_index_query(self._conventions, self._index_query)
        )

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            return

        json_node = json.loads(response)
        results = json_node.get("Results", None)
        if results is None:
            self._throw_invalid_response()
            return

        self.result = [ExplainQueryResult.from_json(res) for res in results]

    def is_read_request(self) -> bool:
        return True
