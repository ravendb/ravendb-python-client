from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from ravendb.documents.operations.definitions import IOperation
from ravendb.documents.operations.patch import PatchStatus
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.http.http_cache import HttpCache


def escape_json_pointer_segment(segment: str) -> str:
    """Escapes one path segment per RFC 6901: '~' becomes '~0' and '/' becomes '~1'."""
    return segment.replace("~", "~0").replace("/", "~1")


class JsonPatchDocument:
    """
    A list of RFC 6902 operations to apply to one document, in order.

    Paths are JSON pointers: ``/Name``, ``/Address/City``, ``/Items/0``. ``/Items/-``
    means "append to the array". Build one with the methods below and hand it to
    :class:`JsonPatchOperation`.
    """

    def __init__(self, operations: List[Dict[str, Any]] = None):
        self.operations: List[Dict[str, Any]] = operations or []

    def add(self, path: str, value: Any) -> JsonPatchDocument:
        """Creates a member, or replaces it when it already exists. Use '/array/-' to append."""
        self.operations.append({"op": "add", "path": path, "value": value})
        return self

    def remove(self, path: str) -> JsonPatchDocument:
        self.operations.append({"op": "remove", "path": path})
        return self

    def replace(self, path: str, value: Any) -> JsonPatchDocument:
        """Overwrites what is already at the path. Fails when nothing is there."""
        self.operations.append({"op": "replace", "path": path, "value": value})
        return self

    def move(self, from_path: str, path: str) -> JsonPatchDocument:
        self.operations.append({"op": "move", "from": from_path, "path": path})
        return self

    def copy(self, from_path: str, path: str) -> JsonPatchDocument:
        self.operations.append({"op": "copy", "from": from_path, "path": path})
        return self

    def test(self, path: str, value: Any) -> JsonPatchDocument:
        """Fails the whole patch unless the path already holds this value."""
        self.operations.append({"op": "test", "path": path, "value": value})
        return self

    def to_json(self) -> List[Dict[str, Any]]:
        return list(self.operations)

    @classmethod
    def from_json(cls, json_list: Optional[List[Dict[str, Any]]]) -> JsonPatchDocument:
        return cls(list(json_list or []))

    def __len__(self) -> int:
        return len(self.operations)

    def __repr__(self) -> str:
        return f"JsonPatchDocument({self.operations!r})"


class JsonPatchResult:
    def __init__(
        self,
        status: Optional[PatchStatus] = None,
        modified_document: Optional[dict] = None,
        original_document: Optional[dict] = None,
        debug: Optional[dict] = None,
    ):
        self.status = status
        self.modified_document = modified_document
        self.original_document = original_document
        self.debug = debug

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> JsonPatchResult:
        status = json_dict.get("Status")
        return cls(
            status=PatchStatus(status) if status else None,
            modified_document=json_dict.get("ModifiedDocument"),
            original_document=json_dict.get("OriginalDocument"),
            debug=json_dict.get("Debug"),
        )


class JsonPatchOperation(IOperation[JsonPatchResult]):
    """
    Applies a set of RFC 6902 operations to one document, in the order they were added.

    See https://ravendb.net/docs/article-page/latest/csharp/client-api/operations/patching/json-patch-syntax
    """

    def __init__(self, key: str, json_patch_document: JsonPatchDocument):
        if not key:
            raise ValueError("key cannot be None or empty")
        if json_patch_document is None:
            raise ValueError("json_patch_document cannot be None")

        self._key = key
        self._json_patch_document = json_patch_document

    def get_command(
        self,
        store: "DocumentStore",
        conventions: "DocumentConventions",
        cache: "HttpCache" = None,
    ) -> RavenCommand[JsonPatchResult]:
        return self._JsonPatchCommand(self._key, self._json_patch_document)

    class _JsonPatchCommand(RavenCommand[JsonPatchResult]):
        def __init__(self, key: str, json_patch_document: JsonPatchDocument):
            super().__init__(JsonPatchResult)
            self._key = key
            self._json_patch_document = json_patch_document

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            from ravendb.tools.utils import Utils

            url = f"{node.url}/databases/{node.database}/json-patch?id={Utils.quote_key(self._key)}"

            request = requests.Request("PATCH", url)
            request.data = {"Operations": self._json_patch_document.to_json()}
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = JsonPatchResult.from_json(json.loads(response))
