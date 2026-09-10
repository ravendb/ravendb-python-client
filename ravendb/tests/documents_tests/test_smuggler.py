"""
Tests for the database smuggler: the options that say what an export or import covers,
the request shapes, backup-file ordering for incremental imports, and a full export /
import round trip against a real server.
"""

import io
import json
import os
import tempfile
import unittest

from ravendb.documents.smuggler.common import (
    DEFAULT_OPERATE_ON_DATABASE_RECORD_TYPES,
    DEFAULT_OPERATE_ON_TYPES,
    DatabaseItemType,
    DatabaseRecordItemType,
    DatabaseSmugglerExportOptions,
    DatabaseSmugglerImportOptions,
    DatabaseSmugglerOptions,
    ExportCompressionAlgorithm,
    flags_from_string,
    flags_to_string,
)
from ravendb.documents.smuggler.database_smuggler import DatabaseSmuggler, _backup_sort_key, _is_backup_file
from ravendb.documents.smuggler.result import SmugglerResult
from ravendb.http.server_node import ServerNode
from ravendb.tests.test_base import TestBase, User


class TestItemTypeFlags(unittest.TestCase):
    def test_nothing_selected_renders_as_none(self):
        self.assertEqual("None", flags_to_string(set(), DatabaseItemType))
        self.assertEqual("None", flags_to_string(None, DatabaseItemType))
        self.assertEqual("None", flags_to_string({DatabaseItemType.NONE}, DatabaseItemType))

    def test_members_render_as_a_comma_separated_list(self):
        # This is the shape the server's Enum.Parse reads a [Flags] value back from.
        self.assertEqual(
            "Documents, Indexes",
            flags_to_string({DatabaseItemType.INDEXES, DatabaseItemType.DOCUMENTS}, DatabaseItemType),
        )

    def test_rendering_is_stable_regardless_of_set_order(self):
        one = flags_to_string({DatabaseItemType.DOCUMENTS, DatabaseItemType.ATTACHMENTS}, DatabaseItemType)
        other = flags_to_string({DatabaseItemType.ATTACHMENTS, DatabaseItemType.DOCUMENTS}, DatabaseItemType)

        self.assertEqual(one, other)

    def test_flags_round_trip(self):
        members = {DatabaseItemType.DOCUMENTS, DatabaseItemType.TIME_SERIES_DELETED_RANGES}

        self.assertEqual(members, flags_from_string(flags_to_string(members, DatabaseItemType), DatabaseItemType))

    def test_parsing_tolerates_spacing_and_drops_none(self):
        self.assertEqual(
            {DatabaseItemType.DOCUMENTS, DatabaseItemType.INDEXES},
            flags_from_string("None,Documents ,  Indexes", DatabaseItemType),
        )
        self.assertEqual(set(), flags_from_string("", DatabaseItemType))

    def test_the_database_record_types_the_7_2_5_server_knows(self):
        for name in ("QueueSinks", "CdcSinks", "SchemaValidation", "RemoteAttachments", "AiAgents"):
            self.assertEqual(name, DatabaseRecordItemType(name).value)


class TestSmugglerOptions(unittest.TestCase):
    def test_defaults_match_the_server_side_defaults(self):
        options = DatabaseSmugglerOptions()

        self.assertEqual(DEFAULT_OPERATE_ON_TYPES, options.operate_on_types)
        self.assertEqual(DEFAULT_OPERATE_ON_DATABASE_RECORD_TYPES, options.operate_on_database_record_types)
        self.assertTrue(options.include_expired)
        self.assertFalse(options.include_artificial)
        self.assertTrue(options.include_archived)
        self.assertEqual(10000, options.max_steps_for_transform_script)
        self.assertEqual([], options.collections)

    def test_each_instance_gets_its_own_collections(self):
        # A shared default would leak one export's narrowing into the next.
        first = DatabaseSmugglerOptions()
        first.operate_on_types.add(DatabaseItemType.TOMBSTONES)
        first.collections.append("Orders")

        second = DatabaseSmugglerOptions()
        self.assertNotIn(DatabaseItemType.TOMBSTONES, second.operate_on_types)
        self.assertEqual([], second.collections)

    def test_body_carries_the_selections_as_flag_strings(self):
        options = DatabaseSmugglerOptions(
            operate_on_types={DatabaseItemType.DOCUMENTS},
            operate_on_database_record_types={DatabaseRecordItemType.SETTINGS},
        )
        body = options.to_json()

        self.assertEqual("Documents", body["OperateOnTypes"])
        self.assertEqual("Settings", body["OperateOnDatabaseRecordTypes"])

    def test_options_round_trip(self):
        options = DatabaseSmugglerOptions(
            operate_on_types={DatabaseItemType.DOCUMENTS, DatabaseItemType.ATTACHMENTS},
            collections=["Orders"],
            transform_script="this.Foo = 1;",
            max_read_ops_per_second=200,
            skip_corrupted_data=True,
        )

        self.assertEqual(options.to_json(), DatabaseSmugglerOptions.from_json(options.to_json()).to_json())

    def test_export_options_leave_the_compression_choice_to_the_server(self):
        self.assertNotIn("CompressionAlgorithm", DatabaseSmugglerExportOptions().to_json())

    def test_export_options_send_a_chosen_compression_algorithm(self):
        options = DatabaseSmugglerExportOptions(compression_algorithm=ExportCompressionAlgorithm.GZIP)

        self.assertEqual("Gzip", options.to_json()["CompressionAlgorithm"])
        self.assertEqual(
            ExportCompressionAlgorithm.GZIP,
            DatabaseSmugglerExportOptions.from_json(options.to_json()).compression_algorithm,
        )

    def test_import_options_always_state_the_revision_choice(self):
        self.assertFalse(DatabaseSmugglerImportOptions().to_json()["SkipRevisionCreation"])
        self.assertTrue(DatabaseSmugglerImportOptions(skip_revision_creation=True).to_json()["SkipRevisionCreation"])

    def test_import_options_copy_only_the_shared_settings_of_an_export(self):
        # Collections and the database-record selection stay at the import defaults, the
        # same subset the C# copy constructor carries over.
        source = DatabaseSmugglerOptions(
            operate_on_types={DatabaseItemType.DOCUMENTS},
            operate_on_database_record_types={DatabaseRecordItemType.SETTINGS},
            collections=["Orders"],
            transform_script="this.Foo = 1;",
            include_expired=False,
        )
        copied = DatabaseSmugglerImportOptions.from_options(source)

        self.assertEqual({DatabaseItemType.DOCUMENTS}, copied.operate_on_types)
        self.assertEqual("this.Foo = 1;", copied.transform_script)
        self.assertFalse(copied.include_expired)
        self.assertEqual([], copied.collections)
        self.assertEqual(DEFAULT_OPERATE_ON_DATABASE_RECORD_TYPES, copied.operate_on_database_record_types)


class TestSmugglerRequests(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")

    def test_export_posts_the_options_with_the_operation_id(self):
        options = DatabaseSmugglerExportOptions()
        command = DatabaseSmuggler._ExportCommand(options, io.BytesIO(), 42, "A")
        request = command.create_request(self.node)

        self.assertEqual("POST", request.method)
        self.assertEqual("http://localhost:8080/databases/db/smuggler/export?operationId=42", request.url)
        self.assertEqual(options.to_json(), request.data)
        self.assertEqual("A", command.selected_node_tag)

    def test_import_posts_the_options_and_the_dump_as_two_parts(self):
        options = DatabaseSmugglerImportOptions(skip_revision_creation=True)
        command = DatabaseSmuggler._ImportCommand(options, io.BytesIO(b"dump"), 43, "B")
        request = command.create_request(self.node)

        self.assertEqual("POST", request.method)
        self.assertEqual("http://localhost:8080/databases/db/smuggler/import?operationId=43", request.url)
        # The server reads the options section before the dump, so the order matters.
        self.assertEqual(["importOptions", "file"], list(request.files))
        self.assertTrue(json.loads(request.files["importOptions"][1])["SkipRevisionCreation"])
        self.assertEqual("B", command.selected_node_tag)

    def test_a_smuggler_without_a_database_refuses_to_run(self):
        class _StoreWithoutDatabase:
            database = None

            def get_request_executor(self, database=None):
                return None

        smuggler = DatabaseSmuggler(_StoreWithoutDatabase())
        with self.assertRaises(RuntimeError):
            smuggler.export(DatabaseSmugglerExportOptions(), io.BytesIO())
        with self.assertRaises(RuntimeError):
            smuggler.import_data(DatabaseSmugglerImportOptions(), io.BytesIO())

    def test_missing_arguments_are_rejected_client_side(self):
        smuggler = DatabaseSmuggler(type("_Store", (), {"database": "db"})())

        with self.assertRaises(ValueError):
            smuggler.export(None, io.BytesIO())
        with self.assertRaises(ValueError):
            smuggler.export(DatabaseSmugglerExportOptions(), None)
        with self.assertRaises(ValueError):
            smuggler.import_data(None, io.BytesIO())
        with self.assertRaises(ValueError):
            smuggler.import_data(DatabaseSmugglerImportOptions(), None)


class TestBackupFileOrdering(unittest.TestCase):
    def test_backup_extensions_are_recognized(self):
        for name in (
            "2026-06-16.ravendb-full-backup",
            "2026-06-16.ravendb-incremental-backup",
            "2026-06-16.ravendb-encrypted-full-backup",
            "2026-06-16.ravendb-encrypted-incremental-backup",
            "old.ravendb-full-dump",
            "old.ravendb-incremental-dump",
        ):
            self.assertTrue(_is_backup_file(name), name)

    def test_other_files_are_ignored(self):
        for name in ("notes.txt", "export.ravendbdump", "2026-06-16.ravendb-snapshot"):
            self.assertFalse(_is_backup_file(name), name)

    def test_a_full_backup_sorts_before_its_incrementals(self):
        with tempfile.TemporaryDirectory() as directory:
            names = [
                "2026-06-16-10-00.ravendb-incremental-backup",
                "2026-06-16-10-00.ravendb-full-backup",
                "2026-06-15-10-00.ravendb-full-backup",
            ]
            for name in names:
                open(os.path.join(directory, name), "wb").close()

            ordered = sorted((os.path.join(directory, name) for name in names), key=_backup_sort_key)

            self.assertEqual(
                [
                    "2026-06-15-10-00.ravendb-full-backup",
                    "2026-06-16-10-00.ravendb-full-backup",
                    "2026-06-16-10-00.ravendb-incremental-backup",
                ],
                [os.path.basename(path) for path in ordered],
            )

    def test_incremental_import_narrows_and_restores_the_selection(self):
        # Indexes and subscriptions come from the last file only, so an earlier
        # incremental cannot bring back an index a later backup dropped.
        options = DatabaseSmugglerImportOptions()
        original = DatabaseSmuggler._configure_options_for_incremental_import(options)

        self.assertIn(DatabaseItemType.TOMBSTONES, options.operate_on_types)
        self.assertIn(DatabaseItemType.COMPARE_EXCHANGE_TOMBSTONES, options.operate_on_types)
        self.assertNotIn(DatabaseItemType.INDEXES, options.operate_on_types)
        self.assertNotIn(DatabaseItemType.SUBSCRIPTIONS, options.operate_on_types)
        self.assertIn(DatabaseItemType.INDEXES, original)
        self.assertIn(DatabaseItemType.SUBSCRIPTIONS, original)


class TestSmugglerAgainstServer(TestBase):
    def setUp(self):
        super().setUp()

    def _store_users(self, store, count: int):
        with store.open_session() as session:
            for i in range(count):
                session.store(User(name=f"user-{i}"), f"users/{i}")
            session.save_changes()

    def test_exports_a_database_to_a_file_and_imports_it_into_another(self):
        self._store_users(self.store, 5)

        with tempfile.TemporaryDirectory() as directory:
            dump = os.path.join(directory, "export.ravendbdump")
            self.store.smuggler.export(DatabaseSmugglerExportOptions(), dump).wait_for_completion()

            self.assertTrue(os.path.isfile(dump))
            self.assertGreater(os.path.getsize(dump), 0)

            with self.get_document_store() as target:
                imported = target.smuggler.import_data(DatabaseSmugglerImportOptions(), dump).wait_for_completion()
                self.assertIsInstance(imported, SmugglerResult)
                self.assertEqual(5, imported.documents.read_count)
                self.assertTrue(imported.messages)

                with target.open_session() as session:
                    self.assertEqual("user-3", session.load("users/3", User).name)
                    self.assertEqual(5, len(session.load(["users/0", "users/1", "users/2", "users/3", "users/4"])))

    def test_exports_into_a_stream(self):
        self._store_users(self.store, 2)

        destination = io.BytesIO()
        exported = self.store.smuggler.export(DatabaseSmugglerExportOptions(), destination).wait_for_completion()

        self.assertGreater(len(destination.getvalue()), 0)
        self.assertEqual(2, exported.documents.read_count)
        self.assertIsNotNone(exported.elapsed)

    def test_an_export_narrowed_to_one_collection_leaves_the_rest_behind(self):
        self._store_users(self.store, 3)
        with self.store.open_session() as session:
            session.store({"Name": "an order", "@metadata": {"@collection": "Orders"}}, "orders/1")
            session.save_changes()

        options = DatabaseSmugglerExportOptions(operate_on_types={DatabaseItemType.DOCUMENTS}, collections=["Users"])

        with tempfile.TemporaryDirectory() as directory:
            dump = os.path.join(directory, "users-only.ravendbdump")
            self.store.smuggler.export(options, dump).wait_for_completion()

            with self.get_document_store() as target:
                target.smuggler.import_data(DatabaseSmugglerImportOptions(), dump).wait_for_completion()

                with target.open_session() as session:
                    self.assertIsNotNone(session.load("users/0", User))
                    self.assertIsNone(session.load("orders/1"))

    def test_export_creates_the_target_directory(self):
        self._store_users(self.store, 1)

        with tempfile.TemporaryDirectory() as directory:
            dump = os.path.join(directory, "nested", "deeper", "export.ravendbdump")
            self.store.smuggler.export(DatabaseSmugglerExportOptions(), dump).wait_for_completion()

            self.assertTrue(os.path.isfile(dump))

    def test_for_database_targets_another_database_on_the_same_store(self):
        self._store_users(self.store, 2)

        with self.get_document_store() as target:
            smuggler = self.store.smuggler
            self.assertIs(smuggler, smuggler.for_database(self.store.database))

            with tempfile.TemporaryDirectory() as directory:
                dump = os.path.join(directory, "export.ravendbdump")
                smuggler.export(DatabaseSmugglerExportOptions(), dump).wait_for_completion()

                self.store.smuggler.for_database(target.database).import_data(
                    DatabaseSmugglerImportOptions(), dump
                ).wait_for_completion()

            with target.open_session() as session:
                self.assertEqual("user-1", session.load("users/1", User).name)


class TestSmugglerResult(unittest.TestCase):
    RESPONSE = {
        "Documents": {
            "ReadCount": 5,
            "SkippedCount": 1,
            "ErroredCount": 0,
            "SizeInBytes": 120,
            "LastEtag": 9,
            "Attachments": {"ReadCount": 2, "SizeInBytes": 40},
        },
        "RevisionDocuments": {"ReadCount": 3},
        "Indexes": {"ReadCount": 1},
        "TimeSeriesDeletedRanges": {"ReadCount": 4},
        "DatabaseRecord": {"ReadCount": 1, "QueueSinksUpdated": True, "CdcSinksUpdated": True},
        "Messages": ["Processed 5 documents."],
        "Elapsed": "00:00:01.2340000",
    }

    def test_counts_are_parsed_per_item_type(self):
        result = SmugglerResult.from_json(self.RESPONSE)

        self.assertEqual(5, result.documents.read_count)
        self.assertEqual(1, result.documents.skipped_count)
        self.assertEqual(9, result.documents.last_etag)
        self.assertEqual(3, result.revision_documents.read_count)
        self.assertEqual(1, result.indexes.read_count)
        self.assertEqual(4, result.time_series_deleted_ranges.read_count)

    def test_attachments_hang_off_the_documents_count(self):
        result = SmugglerResult.from_json(self.RESPONSE)

        self.assertEqual(2, result.documents.attachments.read_count)
        self.assertEqual(40, result.documents.attachments.size_in_bytes)

    def test_messages_and_elapsed_are_carried(self):
        result = SmugglerResult.from_json(self.RESPONSE)

        self.assertEqual(["Processed 5 documents."], result.messages)
        self.assertEqual("00:00:01.2340000", result.elapsed)

    def test_the_database_record_reports_only_what_was_written(self):
        record = SmugglerResult.from_json(self.RESPONSE).database_record

        self.assertTrue(record.queue_sinks_updated)
        self.assertTrue(record.cdc_sinks_updated)
        self.assertFalse(record.sorters_updated)
        self.assertEqual(["cdc_sinks_updated", "queue_sinks_updated"], record.updated)

    def test_the_database_record_writes_back_only_the_true_flags(self):
        # This is how the server sends them, and how C# writes them out.
        serialized = SmugglerResult.from_json(self.RESPONSE).database_record.to_json()

        self.assertEqual(["QueueSinksUpdated", "CdcSinksUpdated"], [k for k in serialized if k.endswith("Updated")])

    def test_a_missing_section_reads_as_zero_rather_than_raising(self):
        result = SmugglerResult.from_json({})

        self.assertEqual(0, result.documents.read_count)
        self.assertEqual(0, result.identities.read_count)
        self.assertEqual([], result.messages)
        self.assertEqual([], result.database_record.updated)

    def test_a_null_result_reads_as_an_empty_one(self):
        self.assertEqual(0, SmugglerResult.from_json(None).documents.read_count)

    def test_result_round_trips(self):
        result = SmugglerResult.from_json(self.RESPONSE)

        self.assertEqual(result.to_json(), SmugglerResult.from_json(result.to_json()).to_json())

    def test_every_section_the_server_sends_has_a_home(self):
        keys = set(SmugglerResult().to_json())

        for name in (
            "DatabaseRecord",
            "Documents",
            "RevisionDocuments",
            "Tombstones",
            "Conflicts",
            "Identities",
            "Indexes",
            "CompareExchange",
            "Subscriptions",
            "Counters",
            "CompareExchangeTombstones",
            "TimeSeries",
            "ReplicationHubCertificates",
            "TimeSeriesDeletedRanges",
        ):
            self.assertIn(name, keys)
