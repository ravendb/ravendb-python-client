from __future__ import annotations

import json
import os
import tempfile
from typing import IO, TYPE_CHECKING, List, Optional, Union

import requests

from ravendb.documents.commands.bulkinsert import GetNextOperationIdCommand
from ravendb.documents.operations.operation import Operation
from ravendb.documents.smuggler.common import (
    DatabaseItemType,
    DatabaseSmugglerExportOptions,
    DatabaseSmugglerImportOptions,
    DatabaseSmugglerOptions,
)
from ravendb.documents.smuggler.result import SmugglerResult
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.raven_command import VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.primitives import constants
from ravendb.util.request_utils import RequestUtils

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore

_IMPORT_OPTIONS_PART = "importOptions"
_COPY_BUFFER_SIZE = 8192

_PERIODIC_BACKUP = constants.Documents.PeriodicBackup
_LEGACY_INCREMENTAL_BACKUP_EXTENSION = ".ravendb-incremental-dump"
_LEGACY_FULL_BACKUP_EXTENSION = ".ravendb-full-dump"

# A full backup or snapshot is restored before the incremental files that build on it.
_FULL_BACKUP_EXTENSIONS = (
    _PERIODIC_BACKUP.FULL_BACKUP_EXTENSTION,
    _PERIODIC_BACKUP.ENCRYPTED_FULL_BACKUP_EXTENSTION,
    _PERIODIC_BACKUP.SNAPSHOT_EXTENSTION,
    _PERIODIC_BACKUP.ENCRYPTED_SNAPSHOT_EXTENSTION,
)

# Snapshots are deliberately absent: they are restored, not imported.
_BACKUP_EXTENSIONS = (
    _PERIODIC_BACKUP.FULL_BACKUP_EXTENSTION,
    _PERIODIC_BACKUP.ENCRYPTED_FULL_BACKUP_EXTENSTION,
    _PERIODIC_BACKUP.INCREMENTAL_BACKUP_EXTENSTION,
    _PERIODIC_BACKUP.ENCRYPTED_INCREMENTAL_BACKUP_EXTENSTION,
    _LEGACY_INCREMENTAL_BACKUP_EXTENSION,
    _LEGACY_FULL_BACKUP_EXTENSION,
)


class SmugglerOperation(Operation):
    """An export or import operation, whose result is a typed SmugglerResult."""

    def wait_for_completion(self) -> SmugglerResult:
        return SmugglerResult.from_json(super().wait_for_completion())


def _is_backup_file(file_path: str) -> bool:
    return os.path.splitext(file_path)[1].lower() in _BACKUP_EXTENSIONS


def _backup_sort_key(file_path: str):
    name, extension = os.path.splitext(os.path.basename(file_path))
    # A full backup is restored before the incremental files that build on it.
    return name, 0 if extension.lower() in _FULL_BACKUP_EXTENSIONS else 1, os.path.getmtime(file_path)


class DatabaseSmuggler:
    """
    Exports a database to a file or stream and imports it back, through the server's
    smuggler endpoints. Reached as ``store.smuggler``.
    """

    def __init__(self, store: "DocumentStore", database_name: str = None):
        self._store = store
        self._database_name = database_name if database_name is not None else store.database
        self._request_executor = None

    @property
    def _executor(self):
        if self._request_executor is None and self._database_name is not None:
            self._request_executor = self._store.get_request_executor(self._database_name)
        return self._request_executor

    def for_database(self, database_name: str) -> DatabaseSmuggler:
        if self._database_name is not None and database_name is not None:
            if database_name.lower() == self._database_name.lower():
                return self

        return DatabaseSmuggler(self._store, database_name)

    def _assert_database_set(self) -> None:
        if self._executor is None:
            raise RuntimeError("Cannot use Smuggler without a database defined, did you forget to call for_database?")

    def _next_operation_id(self):
        command = GetNextOperationIdCommand()
        self._executor.execute_command(command)
        return command.result, command.node_tag

    def _operation_for(self, operation_id: int, node_tag: str) -> SmugglerOperation:
        return SmugglerOperation(
            self._executor,
            lambda: None,
            self._executor.conventions,
            operation_id,
            node_tag,
        )

    def export(
        self,
        options: DatabaseSmugglerExportOptions,
        to_file_or_stream: Union[str, IO[bytes]],
    ) -> SmugglerOperation:
        """
        Exports the database. ``to_file_or_stream`` is either a path, in which case the
        file (and any missing parent directory) is created, or a writable binary stream,
        which is left open.

        The download runs to completion before this returns, so the operation handed back
        is only there to read the server-side result and progress.
        """
        if options is None:
            raise ValueError("options cannot be None")
        if to_file_or_stream is None:
            raise ValueError("to_file_or_stream cannot be None")

        self._assert_database_set()

        if isinstance(to_file_or_stream, str):
            directory = os.path.dirname(os.path.abspath(to_file_or_stream))
            if directory and not os.path.isdir(directory):
                os.makedirs(directory, exist_ok=True)

            with open(to_file_or_stream, "wb") as destination:
                return self._export_to_stream(options, destination)

        return self._export_to_stream(options, to_file_or_stream)

    def _export_to_stream(self, options: DatabaseSmugglerExportOptions, destination: IO[bytes]) -> SmugglerOperation:
        operation_id, node_tag = self._next_operation_id()
        self._executor.execute_command(self._ExportCommand(options, destination, operation_id, node_tag))
        return self._operation_for(operation_id, node_tag)

    def export_to_database(
        self,
        options: DatabaseSmugglerExportOptions,
        to_smuggler: DatabaseSmuggler,
    ) -> SmugglerOperation:
        """
        Streams an export straight into another database. Returns the import operation on
        the receiving side, which is the one worth waiting on.
        """
        if options is None:
            raise ValueError("options cannot be None")
        if to_smuggler is None:
            raise ValueError("to_smuggler cannot be None")

        import_options = DatabaseSmugglerImportOptions.from_options(options)

        # The sync client cannot hand one side's response stream to the other side's
        # request body, so the export is staged on disk first.
        with tempfile.TemporaryDirectory() as directory:
            staged = os.path.join(directory, "export.ravendbdump")
            self.export(options, staged).wait_for_completion()
            return to_smuggler.import_data(import_options, staged)

    def import_data(
        self,
        options: DatabaseSmugglerImportOptions,
        from_file_or_stream: Union[str, IO[bytes]],
    ) -> SmugglerOperation:
        """
        Imports a dump. ``from_file_or_stream`` is either a path, which is opened and
        closed here, or a readable binary stream, which is left open.
        """
        if options is None:
            raise ValueError("options cannot be None")
        if from_file_or_stream is None:
            raise ValueError("from_file_or_stream cannot be None")

        self._assert_database_set()

        if isinstance(from_file_or_stream, str):
            with open(from_file_or_stream, "rb") as source:
                return self._import_from_stream(options, source)

        return self._import_from_stream(options, from_file_or_stream)

    def _import_from_stream(self, options: DatabaseSmugglerImportOptions, source: IO[bytes]) -> SmugglerOperation:
        operation_id, node_tag = self._next_operation_id()
        self._executor.execute_command(self._ImportCommand(options, source, operation_id, node_tag))
        return self._operation_for(operation_id, node_tag)

    def import_incremental(self, options: DatabaseSmugglerImportOptions, from_directory: str) -> None:
        """
        Imports a backup directory in order, waiting for each file. Indexes and
        subscriptions come from the last file only, so a later incremental file cannot
        resurrect an index the backup dropped.
        """
        if options is None:
            raise ValueError("options cannot be None")

        files = sorted(
            (
                os.path.join(from_directory, name)
                for name in os.listdir(from_directory)
                if _is_backup_file(os.path.join(from_directory, name))
            ),
            key=_backup_sort_key,
        )

        if not files:
            return

        original_operate_on_types = self._configure_options_for_incremental_import(options)
        for file_path in files[:-1]:
            self.import_data(options, file_path).wait_for_completion()

        options.operate_on_types = original_operate_on_types
        self.import_data(options, files[-1]).wait_for_completion()

    @staticmethod
    def _configure_options_for_incremental_import(options: DatabaseSmugglerOptions) -> set:
        options.operate_on_types.add(DatabaseItemType.TOMBSTONES)
        options.operate_on_types.add(DatabaseItemType.COMPARE_EXCHANGE_TOMBSTONES)

        original = set(options.operate_on_types)
        options.operate_on_types.discard(DatabaseItemType.INDEXES)
        options.operate_on_types.discard(DatabaseItemType.SUBSCRIPTIONS)
        return original

    class _ExportCommand(VoidRavenCommand):
        def __init__(
            self,
            options: DatabaseSmugglerExportOptions,
            destination: IO[bytes],
            operation_id: int,
            node_tag: str = None,
        ):
            super().__init__()
            self._options = options
            self._destination = destination
            self._operation_id = operation_id
            self._selected_node_tag = node_tag

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/smuggler/export?operationId={self._operation_id}"

            request = requests.Request("POST", url)
            request.data = self._options.to_json()
            return request

        def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
            prepared_request = session.prepare_request(request)
            RequestUtils.remove_zstd_encoding(prepared_request)
            return session.send(prepared_request, cert=session.cert, stream=True)

        def process_response(self, cache, response: requests.Response, url) -> ResponseDisposeHandling:
            for chunk in response.iter_content(chunk_size=_COPY_BUFFER_SIZE):
                if chunk:
                    self._destination.write(chunk)

            return ResponseDisposeHandling.AUTOMATIC

    class _ImportCommand(VoidRavenCommand):
        def __init__(
            self,
            options: DatabaseSmugglerImportOptions,
            source: IO[bytes],
            operation_id: int,
            node_tag: str = None,
        ):
            super().__init__()
            self._options = options
            self._source = source
            self._operation_id = operation_id
            self._selected_node_tag = node_tag

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/smuggler/import?operationId={self._operation_id}"

            request = requests.Request("POST", url)
            # The server reads the options section first and the dump second, so the order
            # of these parts matters.
            request.files = {
                _IMPORT_OPTIONS_PART: (None, json.dumps(self._options.to_json()), "application/json"),
                "file": ("name", self._source, "application/octet-stream"),
            }
            return request
