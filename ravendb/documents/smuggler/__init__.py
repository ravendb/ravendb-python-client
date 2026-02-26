from __future__ import annotations

import json
from typing import Optional, Set, List, TYPE_CHECKING

import requests

from ravendb.documents.smuggler.common import DatabaseItemType, DatabaseRecordItemType, ExportCompressionAlgorithm
from ravendb.documents.operations.definitions import OperationIdResult
from ravendb.documents.operations.operation import Operation
from ravendb.http.http_cache import HttpCache
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.util.request_utils import RequestUtils

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


_ALL_TYPES = frozenset(
    {
        DatabaseItemType.DOCUMENTS,
        DatabaseItemType.REVISION_DOCUMENTS,
        DatabaseItemType.INDEXES,
        DatabaseItemType.IDENTITIES,
        DatabaseItemType.CONFLICTS,
        DatabaseItemType.COMPARE_EXCHANGE,
        DatabaseItemType.DATABASE_RECORD,
        DatabaseItemType.ATTACHMENTS,
        DatabaseItemType.COUNTER_GROUPS,
        DatabaseItemType.SUBSCRIPTIONS,
        DatabaseItemType.TIME_SERIES,
        DatabaseItemType.TIME_SERIES_DELETED_RANGES,
        DatabaseItemType.REPLICATION_HUB_CERTIFICATES,
    }
)

_ALL_RECORD_TYPES = frozenset(
    {
        DatabaseRecordItemType.CLIENT,
        DatabaseRecordItemType.CONFLICT_SOLVER_CONFIG,
        DatabaseRecordItemType.EXPIRATION,
        DatabaseRecordItemType.EXTERNAL_REPLICATIONS,
        DatabaseRecordItemType.PERIODIC_BACKUPS,
        DatabaseRecordItemType.RAVEN_CONNECTION_STRINGS,
        DatabaseRecordItemType.RAVEN_ETLS,
        DatabaseRecordItemType.REVISIONS,
        DatabaseRecordItemType.SETTINGS,
        DatabaseRecordItemType.SQL_CONNECTION_STRINGS,
        DatabaseRecordItemType.SORTERS,
        DatabaseRecordItemType.SQL_ETLS,
        DatabaseRecordItemType.HUB_PULL_REPLICATIONS,
        DatabaseRecordItemType.SINK_PULL_REPLICATIONS,
        DatabaseRecordItemType.TIME_SERIES,
        DatabaseRecordItemType.DOCUMENTS_COMPRESSION,
        DatabaseRecordItemType.ANALYZERS,
        DatabaseRecordItemType.LOCK_MODE,
        DatabaseRecordItemType.OLAP_CONNECTION_STRINGS,
        DatabaseRecordItemType.OLAP_ETLS,
        DatabaseRecordItemType.ELASTIC_SEARCH_CONNECTION_STRINGS,
        DatabaseRecordItemType.ELASTIC_SEARCH_ETLS,
        DatabaseRecordItemType.POSTGRE_SQL_INTEGRATION,
        DatabaseRecordItemType.QUEUE_CONNECTION_STRINGS,
        DatabaseRecordItemType.QUEUE_ETLS,
        DatabaseRecordItemType.INDEXES_HISTORY,
        DatabaseRecordItemType.REFRESH,
        DatabaseRecordItemType.DATA_ARCHIVAL,
        DatabaseRecordItemType.QUEUE_SINKS,
        DatabaseRecordItemType.SNOWFLAKE_ETLS,
        DatabaseRecordItemType.SNOWFLAKE_CONNECTION_STRINGS,
        DatabaseRecordItemType.EMBEDDINGS_GENERATIONS,
        DatabaseRecordItemType.AI_CONNECTION_STRINGS,
        DatabaseRecordItemType.GEN_AI_ETLS,
        DatabaseRecordItemType.AI_AGENTS,
        DatabaseRecordItemType.REMOTE_ATTACHMENTS,
        DatabaseRecordItemType.SCHEMA_VALIDATION,
    }
)


class DatabaseSmugglerOptions:
    def __init__(
        self,
        operate_on_types: Optional[Set[DatabaseItemType]] = None,
        operate_on_database_record_types: Optional[Set[DatabaseRecordItemType]] = None,
        include_expired: bool = True,
        include_artificial: bool = False,
        include_archived: bool = True,
        remove_analyzers: bool = False,
        transform_script: Optional[str] = None,
        max_steps_for_transform_script: int = 10_000,
        encryption_key: Optional[str] = None,
        max_read_ops_per_second: Optional[int] = None,
        skip_corrupted_data: bool = False,
        collections: Optional[List[str]] = None,
    ):
        self.operate_on_types: Set[DatabaseItemType] = (
            operate_on_types if operate_on_types is not None else set(_ALL_TYPES)
        )
        self.operate_on_database_record_types: Set[DatabaseRecordItemType] = (
            operate_on_database_record_types if operate_on_database_record_types is not None else set(_ALL_RECORD_TYPES)
        )
        self.include_expired = include_expired
        self.include_artificial = include_artificial
        self.include_archived = include_archived
        self.remove_analyzers = remove_analyzers
        self.transform_script = transform_script
        self.max_steps_for_transform_script = max_steps_for_transform_script
        self.encryption_key = encryption_key
        self.max_read_ops_per_second = max_read_ops_per_second
        self.skip_corrupted_data = skip_corrupted_data
        self.collections: List[str] = collections if collections is not None else []

    def to_json(self) -> dict:
        data: dict = {
            "OperateOnTypes": [t.value for t in self.operate_on_types],
            "OperateOnDatabaseRecordTypes": [t.value for t in self.operate_on_database_record_types],
            "IncludeExpired": self.include_expired,
            "IncludeArtificial": self.include_artificial,
            "IncludeArchived": self.include_archived,
            "RemoveAnalyzers": self.remove_analyzers,
            "MaxStepsForTransformScript": self.max_steps_for_transform_script,
            "SkipCorruptedData": self.skip_corrupted_data,
            "Collections": self.collections,
        }
        if self.transform_script is not None:
            data["TransformScript"] = self.transform_script
        if self.encryption_key is not None:
            data["EncryptionKey"] = self.encryption_key
        if self.max_read_ops_per_second is not None:
            data["MaxReadOpsPerSecond"] = self.max_read_ops_per_second
        return data


class DatabaseSmugglerExportOptions(DatabaseSmugglerOptions):
    def __init__(
        self,
        operate_on_types: Optional[Set[DatabaseItemType]] = None,
        operate_on_database_record_types: Optional[Set[DatabaseRecordItemType]] = None,
        include_expired: bool = True,
        include_artificial: bool = False,
        include_archived: bool = True,
        remove_analyzers: bool = False,
        transform_script: Optional[str] = None,
        max_steps_for_transform_script: int = 10_000,
        encryption_key: Optional[str] = None,
        max_read_ops_per_second: Optional[int] = None,
        skip_corrupted_data: bool = False,
        collections: Optional[List[str]] = None,
        compression_algorithm: Optional[ExportCompressionAlgorithm] = None,
    ):
        super().__init__(
            operate_on_types=operate_on_types,
            operate_on_database_record_types=operate_on_database_record_types,
            include_expired=include_expired,
            include_artificial=include_artificial,
            include_archived=include_archived,
            remove_analyzers=remove_analyzers,
            transform_script=transform_script,
            max_steps_for_transform_script=max_steps_for_transform_script,
            encryption_key=encryption_key,
            max_read_ops_per_second=max_read_ops_per_second,
            skip_corrupted_data=skip_corrupted_data,
            collections=collections,
        )
        self.compression_algorithm: Optional[ExportCompressionAlgorithm] = compression_algorithm

    def to_json(self) -> dict:
        data = super().to_json()
        if self.compression_algorithm is not None:
            data["CompressionAlgorithm"] = self.compression_algorithm.value
        return data


class DatabaseSmugglerImportOptions(DatabaseSmugglerOptions):
    def __init__(
        self,
        operate_on_types: Optional[Set[DatabaseItemType]] = None,
        operate_on_database_record_types: Optional[Set[DatabaseRecordItemType]] = None,
        include_expired: bool = True,
        include_artificial: bool = False,
        include_archived: bool = True,
        remove_analyzers: bool = False,
        transform_script: Optional[str] = None,
        max_steps_for_transform_script: int = 10_000,
        encryption_key: Optional[str] = None,
        max_read_ops_per_second: Optional[int] = None,
        skip_corrupted_data: bool = False,
        collections: Optional[List[str]] = None,
        skip_revision_creation: bool = False,
    ):
        super().__init__(
            operate_on_types=operate_on_types,
            operate_on_database_record_types=operate_on_database_record_types,
            include_expired=include_expired,
            include_artificial=include_artificial,
            include_archived=include_archived,
            remove_analyzers=remove_analyzers,
            transform_script=transform_script,
            max_steps_for_transform_script=max_steps_for_transform_script,
            encryption_key=encryption_key,
            max_read_ops_per_second=max_read_ops_per_second,
            skip_corrupted_data=skip_corrupted_data,
            collections=collections,
        )
        self.skip_revision_creation = skip_revision_creation

    def to_json(self) -> dict:
        data = super().to_json()
        data["SkipRevisionCreation"] = self.skip_revision_creation
        return data


class DatabaseSmuggler:
    def __init__(self, store: "DocumentStore", database_name: Optional[str] = None):
        self._store = store
        self._database_name = database_name or store.database

    def for_database(self, database_name: str) -> "DatabaseSmuggler":
        if database_name and database_name.lower() == (self._database_name or "").lower():
            return self
        return DatabaseSmuggler(self._store, database_name)

    def export(self, options: DatabaseSmugglerExportOptions, to_file: str) -> None:
        """Export database contents to a file path."""
        self._store.assert_initialized()
        request_executor = self._store.get_request_executor(self._database_name)

        class _ExportCommand(VoidRavenCommand):
            def __init__(self, export_options: DatabaseSmugglerExportOptions, file_path: str):
                super().__init__()
                self._options = export_options
                self._file_path = file_path

            def create_request(self, node: ServerNode) -> requests.Request:
                url = f"{node.url}/databases/{node.database}/smuggler/export"
                req = requests.Request("POST", url)
                req.data = json.dumps(self._options.to_json())
                req.headers = {"Content-Type": "application/json"}
                return req

            def send(self, session: requests.Session, request):
                prepared = session.prepare_request(request)
                RequestUtils.remove_zstd_encoding(prepared)
                return session.send(prepared, stream=True, cert=session.cert)

            def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
                with open(self._file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return ResponseDisposeHandling.AUTOMATIC

        command = _ExportCommand(options, to_file)
        request_executor.execute_command(command)

    def import_data(self, options: DatabaseSmugglerImportOptions, from_file: str) -> None:
        """Import database contents from a file path."""
        self._store.assert_initialized()
        request_executor = self._store.get_request_executor(self._database_name)

        class _ImportCommand(RavenCommand[OperationIdResult]):
            def __init__(self, import_options: DatabaseSmugglerImportOptions, file_path: str):
                super().__init__(OperationIdResult)
                self._options = import_options
                self._file_path = file_path

            def create_request(self, node: ServerNode) -> requests.Request:
                url = f"{node.url}/databases/{node.database}/smuggler/import"
                return requests.Request("POST", url)

            def send(self, session: requests.Session, request):
                options_json = json.dumps(self._options.to_json())
                with open(self._file_path, "rb") as fh:
                    request.files = {
                        "importOptions": (None, options_json, "application/json"),
                        "file": ("file", fh, "application/octet-stream"),
                    }
                    return super().send(session, request)

            def set_response(self, response: Optional[str], from_cache: bool) -> None:
                self.result = OperationIdResult.from_json(json.loads(response))

            def is_read_request(self) -> bool:
                return False

        command = _ImportCommand(options, from_file)
        request_executor.execute_command(command)
        node_tag = command.selected_node_tag or command.result.operation_node_tag
        Operation(
            request_executor,
            lambda: None,
            request_executor.conventions,
            command.result.operation_id,
            node_tag,
        ).wait_for_completion()
