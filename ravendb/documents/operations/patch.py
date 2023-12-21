from __future__ import annotations
import datetime
import json
from enum import Enum
from typing import Union, Optional, Dict, Generic, TYPE_CHECKING

import requests

from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import OperationIdResult, IOperation, _Operation_T
from ravendb.documents.operations.misc import QueryOperationOptions
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


class PatchRequest:
    def __init__(self, script: Optional[str] = "", values: Optional[Dict[str, object]] = None):
        self.values: Dict[str, object] = values or {}
        self.script = script

    @classmethod
    def for_script(cls, script: str) -> PatchRequest:
        request = cls()
        request.script = script
        return request

    def serialize(self):
        return {"Script": self.script, "Values": self.values}


class PatchStatus(Enum):
    DOCUMENT_DOES_NOT_EXIST = "DocumentDoesNotExist"
    CREATED = "Created"
    PATCHED = "Patched"
    SKIPPED = "Skipped"
    NOT_MODIFIED = "NotModified"

    def __str__(self):
        return self.value


class PatchResult:
    def __init__(
        self,
        status: Optional[PatchStatus] = None,
        modified_document: Optional[dict] = None,
        original_document: Optional[dict] = None,
        debug: Optional[dict] = None,
        last_modified: Optional[datetime.datetime] = None,
        change_vector: Optional[str] = None,
        collection: Optional[str] = None,
    ):
        self.status = status
        self.modified_document = modified_document
        self.original_document = original_document
        self.debug = debug
        self.last_modified = last_modified
        self.change_vector = change_vector
        self.collection = collection

    @classmethod
    def from_json(cls, json_dict: dict) -> PatchResult:
        return cls(
            PatchStatus(json_dict["Status"]),
            json_dict.get("ModifiedDocument", None),
            json_dict.get("OriginalDocument", None),
            json_dict.get("Debug", None),
            Utils.string_to_datetime(json_dict["LastModified"]) if "LastModified" in json_dict else None,
            json_dict.get("ChangeVector", None),
            json_dict.get("Collection", None),
        )


class PatchOperation(IOperation[PatchResult]):
    class Payload:
        def __init__(self, patch: PatchRequest, patch_if_missing: PatchResult):
            self._patch = patch
            self._patch_if_missing = patch_if_missing

        @property
        def patch(self):
            return self._patch

        @property
        def patch_if_missing(self):
            return self._patch_if_missing

    class Result(Generic[_Operation_T]):
        def __init__(self, status: PatchStatus = None, document: _Operation_T = None):
            self.status = status
            self.document = document

    def __init__(
        self,
        key: str,
        change_vector: str,
        patch: PatchRequest,
        patch_if_missing: Optional[PatchRequest] = None,
        skip_patch_if_change_vector_mismatch: Optional[bool] = None,
    ):
        if not patch:
            raise ValueError("Patch cannot be None")

        if patch.script.isspace():
            raise ValueError("Patch script cannot be None or whitespace")

        if patch_if_missing and patch_if_missing.script.isspace():
            raise ValueError("PatchIfMissing script cannot be None or whitespace")

        self._key = key
        self._change_vector = change_vector
        self._patch = patch
        self._patch_if_missing = patch_if_missing
        self._skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch

    def get_command(
        self,
        store: "DocumentStore",
        conventions: DocumentConventions,
        cache: HttpCache,
        return_debug_information: Optional[bool] = False,
        test: Optional[bool] = False,
    ) -> RavenCommand[_Operation_T]:
        return self.PatchCommand(
            conventions,
            self._key,
            self._change_vector,
            self._patch,
            self._patch_if_missing,
            self._skip_patch_if_change_vector_mismatch,
            return_debug_information,
            test,
        )

    class PatchCommand(RavenCommand[PatchResult]):
        def __init__(
            self,
            conventions: "DocumentConventions",
            document_id: str,
            change_vector: str,
            patch: PatchRequest,
            patch_if_missing: PatchRequest,
            skip_patch_if_change_vector_mismatch: Optional[bool] = False,
            return_debug_information: Optional[bool] = False,
            test: Optional[bool] = False,
        ):
            if conventions is None:
                raise ValueError("None conventions are invalid")
            if document_id is None:
                raise ValueError("None key is invalid")
            if patch is None:
                raise ValueError("None patch is invalid")
            if patch_if_missing and not patch_if_missing.script:
                raise ValueError("None or Empty script is invalid")
            if patch and patch.script.isspace():
                raise ValueError("None or empty patch_if_missing.script is invalid")
            if not document_id:
                raise ValueError("Document_id canoot be None")

            super().__init__(PatchResult)
            self._conventions = conventions
            self._document_id = document_id
            self._change_vector = change_vector
            self._patch = patch
            self._patch_if_missing = patch_if_missing
            self._skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch
            self._return_debug_information = return_debug_information
            self._test = test

        def create_request(self, server_node: ServerNode) -> requests.Request:
            path = f"docs?id={Utils.quote_key(self._document_id)}"
            if self._skip_patch_if_change_vector_mismatch:
                path += "&skipPatchIfChangeVectorMismatch=true"
            if self._return_debug_information:
                path += "&debug=true"
            if self._test:
                path += "&test=true"
            url = f"{server_node.url}/databases/{server_node.database}/{path}"
            request = requests.Request("PATCH", url)
            if self._change_vector is not None:
                request.headers = {"If-Match": f'"{self._change_vector}"'}

            request.data = {
                "Patch": self._patch.serialize(),
                "PatchIfMissing": self._patch_if_missing.serialize() if self._patch_if_missing else None,
            }

            return request

        def is_read_request(self) -> bool:
            return False

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return

            self.result = PatchResult.from_json(json.loads(response))


class PatchByQueryOperation(IOperation[OperationIdResult]):
    def __init__(self, query_to_update: Union[IndexQuery, str], options: Optional[QueryOperationOptions] = None):
        if query_to_update is None:
            raise ValueError("Invalid query")
        super().__init__()
        if isinstance(query_to_update, str):
            query_to_update = IndexQuery(query=query_to_update)
        self._query_to_update = query_to_update
        if options is None:
            options = QueryOperationOptions()
        self._options = options

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: Optional[HttpCache] = None
    ) -> RavenCommand[OperationIdResult]:
        return self.__PatchByQueryCommand(conventions, self._query_to_update, self._options)

    class __PatchByQueryCommand(RavenCommand[OperationIdResult]):
        def __init__(
            self, conventions: "DocumentConventions", query_to_update: IndexQuery, options: QueryOperationOptions
        ):
            super().__init__(OperationIdResult)
            self._conventions = conventions
            self._query_to_update = query_to_update
            self._options = options

        def create_request(self, server_node) -> requests.Request:
            if not isinstance(self._query_to_update, IndexQuery):
                raise ValueError("query must be IndexQuery Type")

            url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = (
                f"?allowStale={self._options.allow_stale}&maxOpsPerSec={self._options.max_ops_per_sec or ''}"
                f"&details={self._options.retrieve_details}"
            )
            if self._options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self._options.stale_timeout)

            url += path
            data = {"Query": self._query_to_update.to_json()}
            return requests.Request("PATCH", url, data=data)

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            response = json.loads(response)
            self.result = OperationIdResult(response["OperationId"], response["OperationNodeTag"])

        def is_read_request(self) -> bool:
            return False
