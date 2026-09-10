from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from ravendb.documents.operations.cdc_sink.configuration import CdcColumnType
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class CdcSinkSourceColumn:
    """
    One source column as CDC schema discovery sees it. The field names mirror
    CdcColumnMapping so a discovered column can be turned into a mapping directly.
    """

    def __init__(
        self,
        name: str = None,
        native_type: str = None,
        suggested_type: CdcColumnType = CdcColumnType.DEFAULT,
        is_primary_key: bool = False,
        is_cdc_capturable: bool = False,
        unsupported_reason: str = None,
    ):
        self.name = name
        # The source-side type as the database reports it, e.g. "varchar", "jsonb".
        self.native_type = native_type
        self.suggested_type = suggested_type
        self.is_primary_key = is_primary_key
        # False when the source type has no CDC mapping, or the column is not in
        # SQL Server's capture list. The reason is then in unsupported_reason.
        self.is_cdc_capturable = is_cdc_capturable
        self.unsupported_reason = unsupported_reason

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "NativeType": self.native_type,
            "SuggestedType": self.suggested_type.value if self.suggested_type else None,
            "IsPrimaryKey": self.is_primary_key,
            "IsCdcCapturable": self.is_cdc_capturable,
            "UnsupportedReason": self.unsupported_reason,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkSourceColumn:
        suggested_type = json_dict.get("SuggestedType")
        return cls(
            name=json_dict.get("Name"),
            native_type=json_dict.get("NativeType"),
            suggested_type=CdcColumnType(suggested_type) if suggested_type else CdcColumnType.DEFAULT,
            is_primary_key=json_dict.get("IsPrimaryKey", False),
            is_cdc_capturable=json_dict.get("IsCdcCapturable", False),
            unsupported_reason=json_dict.get("UnsupportedReason"),
        )


class CdcSinkSourceForeignKey:
    """A foreign key leaving a source table, which is what a linked table is built from."""

    def __init__(
        self,
        columns: List[str] = None,
        referenced_schema: str = None,
        referenced_table: str = None,
        referenced_columns: List[str] = None,
    ):
        self.columns = columns or []
        self.referenced_schema = referenced_schema
        self.referenced_table = referenced_table
        self.referenced_columns = referenced_columns or []

    def to_json(self) -> Dict[str, Any]:
        return {
            "Columns": self.columns,
            "ReferencedSchema": self.referenced_schema,
            "ReferencedTable": self.referenced_table,
            "ReferencedColumns": self.referenced_columns,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkSourceForeignKey:
        return cls(
            columns=json_dict.get("Columns") or [],
            referenced_schema=json_dict.get("ReferencedSchema"),
            referenced_table=json_dict.get("ReferencedTable"),
            referenced_columns=json_dict.get("ReferencedColumns") or [],
        )


class CdcSinkSourceTable:
    """One source table as CDC schema discovery sees it, annotated with capturability."""

    def __init__(
        self,
        source_table_schema: str = None,
        source_table_name: str = None,
        columns: List[CdcSinkSourceColumn] = None,
        primary_key_columns: List[str] = None,
        foreign_keys: List[CdcSinkSourceForeignKey] = None,
        is_cdc_enabled: bool = False,
        unsupported_reason: str = None,
        warnings: List[str] = None,
    ):
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.columns = columns or []
        self.primary_key_columns = primary_key_columns or []
        self.foreign_keys = foreign_keys or []
        # Whether CDC tracking is already active at the source. Always true for
        # PostgreSQL and MySQL, where membership is a database-level concern.
        self.is_cdc_enabled = is_cdc_enabled
        # Set when the whole table cannot be captured.
        self.unsupported_reason = unsupported_reason
        # Table-scoped findings that do not make it unusable, such as a REPLICA
        # IDENTITY that will not carry row-identifying columns on DELETE.
        self.warnings = warnings or []

    def to_json(self) -> Dict[str, Any]:
        return {
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "Columns": [column.to_json() for column in self.columns],
            "PrimaryKeyColumns": self.primary_key_columns,
            "ForeignKeys": [foreign_key.to_json() for foreign_key in self.foreign_keys],
            "IsCdcEnabled": self.is_cdc_enabled,
            "UnsupportedReason": self.unsupported_reason,
            "Warnings": self.warnings,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkSourceTable:
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            columns=[CdcSinkSourceColumn.from_json(column) for column in json_dict.get("Columns") or []],
            primary_key_columns=json_dict.get("PrimaryKeyColumns") or [],
            foreign_keys=[
                CdcSinkSourceForeignKey.from_json(foreign_key) for foreign_key in json_dict.get("ForeignKeys") or []
            ],
            is_cdc_enabled=json_dict.get("IsCdcEnabled", False),
            unsupported_reason=json_dict.get("UnsupportedReason"),
            warnings=json_dict.get("Warnings") or [],
        )


class CdcSinkSourceSchema:
    """The source database's tables, columns, keys and CDC readiness."""

    def __init__(
        self,
        catalog_name: str = None,
        tables: List[CdcSinkSourceTable] = None,
        errors: List[str] = None,
        has_permission_to_setup: bool = False,
        warnings: List[str] = None,
    ):
        self.catalog_name = catalog_name
        self.tables = tables or []
        # Whole-request failures: validation, an unreachable source, or a connection-level
        # blocker such as PostgreSQL wal_level not being logical.
        self.errors = errors or []
        # Whether the connecting user can provision CDC itself. Distinct from a table's
        # is_cdc_enabled, which says whether CDC is already running.
        self.has_permission_to_setup = has_permission_to_setup
        self.warnings = warnings or []

    @property
    def success(self) -> bool:
        """True when nothing blocks setting CDC up. Warnings do not count against it."""
        return len(self.errors) == 0

    def to_json(self) -> Dict[str, Any]:
        return {
            "CatalogName": self.catalog_name,
            "Tables": [table.to_json() for table in self.tables],
            "Errors": self.errors,
            "HasPermissionToSetup": self.has_permission_to_setup,
            "Warnings": self.warnings,
            "Success": self.success,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkSourceSchema:
        return cls(
            catalog_name=json_dict.get("CatalogName"),
            tables=[CdcSinkSourceTable.from_json(table) for table in json_dict.get("Tables") or []],
            errors=json_dict.get("Errors") or [],
            has_permission_to_setup=json_dict.get("HasPermissionToSetup", False),
            warnings=json_dict.get("Warnings") or [],
        )


class CdcSinkSchemaRequest:
    """The body of a schema-discovery call. Give it either a connection or a name."""

    def __init__(
        self,
        connection: SqlConnectionString = None,
        connection_string_name: str = None,
        schemas: List[str] = None,
    ):
        # Inline credentials, for when the connection has not been saved to the database
        # record yet. When set, connection_string_name is ignored.
        self.connection = connection
        self.connection_string_name = connection_string_name
        # PostgreSQL only, defaults to ["public"] server-side. Each entry is validated
        # against ^[A-Za-z_][A-Za-z0-9_]*$, so quoted names with hyphens are rejected.
        self.schemas = schemas

    def to_json(self) -> Dict[str, Any]:
        return {
            "Connection": self.connection.to_json() if self.connection else None,
            "ConnectionStringName": self.connection_string_name,
            "Schemas": self.schemas,
        }


class GetCdcSinkSchemaOperation(MaintenanceOperation[CdcSinkSourceSchema]):
    """
    Browses the source database a CDC Sink would read: tables, columns, primary and
    foreign keys, each annotated with what CDC can capture. Requires DatabaseAdmin.
    """

    def __init__(
        self,
        connection_or_name: Any = None,
        schemas: List[str] = None,
        request: CdcSinkSchemaRequest = None,
    ):
        if request is not None:
            self._request = request
        elif isinstance(connection_or_name, SqlConnectionString):
            self._request = CdcSinkSchemaRequest(connection=connection_or_name, schemas=schemas)
        elif isinstance(connection_or_name, str):
            self._request = CdcSinkSchemaRequest(connection_string_name=connection_or_name, schemas=schemas)
        else:
            raise ValueError("Pass either a SqlConnectionString, a connection string name, or a request")

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[CdcSinkSourceSchema]:
        return self._GetCdcSinkSchemaCommand(self._request)

    class _GetCdcSinkSchemaCommand(RavenCommand[CdcSinkSourceSchema]):
        def __init__(self, request: CdcSinkSchemaRequest):
            super().__init__(CdcSinkSourceSchema)
            self._request = request

        def is_read_request(self) -> bool:
            # A POST that changes nothing server-side, so failover to the fastest node is fine.
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/cdc-sink/schema"

            request = requests.Request("POST", url)
            request.data = self._request.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = CdcSinkSourceSchema.from_json(json.loads(response))
