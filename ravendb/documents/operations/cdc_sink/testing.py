"""
The CDC Sink mapping preview: run rows from the source table through the configured
mapping and patches without saving anything, to see what documents would come out.

Named ``testing`` rather than ``test`` so ``unittest discover`` does not pick the module
up as a test file.
"""

from __future__ import annotations

import enum
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from ravendb.documents.operations.cdc_sink.configuration import CdcSinkConfiguration
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class TestCdcSinkRowSelector(enum.Enum):
    """Sample the first N rows by primary key, or fetch one row by its key values."""

    FIRST = "First"
    BY_PRIMARY_KEY = "ByPrimaryKey"

    def __str__(self) -> str:
        return self.value


class TestCdcSinkOperation(enum.Enum):
    """Whether to drive each row through the table's Patch or its OnDelete.Patch."""

    UPSERT = "Upsert"
    DELETE = "Delete"

    def __str__(self) -> str:
        return self.value


class TestCdcSinkRowResult:
    """
    One row's worth of preview output. ``document`` and ``source_row`` come back as JSON
    text rather than parsed objects, so parse them yourself when you need the values.
    """

    def __init__(
        self,
        document_id: str = None,
        document: str = None,
        source_row: str = None,
        would_delete: bool = False,
        ignore_deletes: bool = False,
        debug_output: List[str] = None,
        error: str = None,
    ):
        self.document_id = document_id
        # The mapped and patched document, as JSON text. Stays at the pre-patch mapping
        # if the script called del() or put().
        self.document = document
        self.source_row = source_row
        # True for a Delete run when OnDelete.IgnoreDeletes is false.
        self.would_delete = would_delete
        self.ignore_deletes = ignore_deletes
        # Whatever the patch script passed to output().
        self.debug_output = debug_output
        # Set when this row failed on its own, for example the patch threw.
        self.error = error

    def to_json(self) -> Dict[str, Any]:
        return {
            "DocumentId": self.document_id,
            "Document": self.document,
            "SourceRow": self.source_row,
            "WouldDelete": self.would_delete,
            "IgnoreDeletes": self.ignore_deletes,
            "DebugOutput": self.debug_output,
            "Error": self.error,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TestCdcSinkRowResult:
        return cls(
            document_id=json_dict.get("DocumentId"),
            document=json_dict.get("Document"),
            source_row=json_dict.get("SourceRow"),
            would_delete=json_dict.get("WouldDelete", False),
            ignore_deletes=json_dict.get("IgnoreDeletes", False),
            debug_output=json_dict.get("DebugOutput"),
            error=json_dict.get("Error"),
        )


class TestCdcSinkMappingResult:
    """
    Always an array shape, so a single-row test and a multi-row sample read the same.
    A row that failed on its own carries its error; a failure that stopped the whole
    request lands in ``errors`` and leaves ``results`` empty.
    """

    def __init__(
        self,
        results: List[TestCdcSinkRowResult] = None,
        errors: List[str] = None,
        warnings: List[str] = None,
    ):
        self.results = results or []
        self.errors = errors or []
        # Advisory notes that do not invalidate the results, for example that linked and
        # embedded tables are not exercised in test mode.
        self.warnings = warnings or []

    def to_json(self) -> Dict[str, Any]:
        return {
            "Results": [result.to_json() for result in self.results],
            "Errors": self.errors,
            "Warnings": self.warnings,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TestCdcSinkMappingResult:
        return cls(
            results=[TestCdcSinkRowResult.from_json(result) for result in json_dict.get("Results") or []],
            errors=json_dict.get("Errors") or [],
            warnings=json_dict.get("Warnings") or [],
        )


class TestCdcSinkMappingRequest:
    """The body of a mapping preview: the task configuration plus which rows to run."""

    def __init__(
        self,
        configuration: CdcSinkConfiguration = None,
        connection: SqlConnectionString = None,
        source_table_schema: str = None,
        source_table_name: str = None,
        row_selector: TestCdcSinkRowSelector = TestCdcSinkRowSelector.FIRST,
        primary_key_values: List[str] = None,
        operation: TestCdcSinkOperation = TestCdcSinkOperation.UPSERT,
        max_rows: int = 1,
    ):
        # The same configuration used to create the task: driver, table lookup, column
        # mapping and patch scripts all come from here.
        self.configuration = configuration
        # Inline credentials, for when the connection has not been saved yet. When null,
        # the configuration's connection_string_name is used.
        self.connection = connection
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.row_selector = row_selector
        # In the target table's primary key order. Required with BY_PRIMARY_KEY.
        self.primary_key_values = primary_key_values
        self.operation = operation
        # Only meaningful with FIRST, and must be 1 with BY_PRIMARY_KEY. Capped at 5,000
        # server-side: this endpoint is a preview, not a bulk fetch.
        self.max_rows = max_rows

    def to_json(self) -> Dict[str, Any]:
        return {
            "Configuration": self.configuration.to_json() if self.configuration else None,
            "Connection": self.connection.to_json() if self.connection else None,
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "RowSelector": self.row_selector.value if self.row_selector else None,
            "PrimaryKeyValues": self.primary_key_values,
            "Operation": self.operation.value if self.operation else None,
            "MaxRows": self.max_rows,
        }


class TestCdcSinkMappingOperation(MaintenanceOperation[TestCdcSinkMappingResult]):
    """
    Previews how source rows would become documents, before saving a CDC Sink task.
    Requires DatabaseAdmin.
    """

    def __init__(self, request: TestCdcSinkMappingRequest):
        if request is None:
            raise ValueError("request cannot be None")

        self._request = request

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[TestCdcSinkMappingResult]:
        return self._TestCdcSinkMappingCommand(self._request)

    class _TestCdcSinkMappingCommand(RavenCommand[TestCdcSinkMappingResult]):
        def __init__(self, request: TestCdcSinkMappingRequest):
            super().__init__(TestCdcSinkMappingResult)
            self._request = request

        def is_read_request(self) -> bool:
            # A POST that changes nothing server-side, so failover to the fastest node is fine.
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/cdc-sink/test"

            request = requests.Request("POST", url)
            request.data = self._request.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = TestCdcSinkMappingResult.from_json(json.loads(response))
