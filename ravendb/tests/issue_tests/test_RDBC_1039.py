"""
RDBC-1039: store.smuggler property and DatabaseSmuggler class.

C# reference: IDocumentStore.Smuggler, DatabaseSmuggler.ExportAsync() / ImportAsync()
"""

import unittest

from ravendb.documents.smuggler import (
    DatabaseSmuggler,
    DatabaseSmugglerExportOptions,
    DatabaseSmugglerImportOptions,
    DatabaseSmugglerOptions,
)
from ravendb.documents.smuggler.common import DatabaseItemType, DatabaseRecordItemType, ExportCompressionAlgorithm
from ravendb.tests.test_base import TestBase


class TestDatabaseSmugglerUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_export_options_defaults_include_all_types(self):
        options = DatabaseSmugglerExportOptions()
        self.assertIsNotNone(options.operate_on_types)
        self.assertIn(DatabaseItemType.DOCUMENTS, options.operate_on_types)
        self.assertIn(DatabaseItemType.INDEXES, options.operate_on_types)
        self.assertIn(DatabaseItemType.COUNTER_GROUPS, options.operate_on_types)
        self.assertIn(DatabaseItemType.REPLICATION_HUB_CERTIFICATES, options.operate_on_types)
        self.assertIn(DatabaseItemType.TIME_SERIES_DELETED_RANGES, options.operate_on_types)
        # TOMBSTONES and COMPARE_EXCHANGE_TOMBSTONES are not in the default set
        self.assertNotIn(DatabaseItemType.TOMBSTONES, options.operate_on_types)
        self.assertNotIn(DatabaseItemType.COMPARE_EXCHANGE_TOMBSTONES, options.operate_on_types)

    def test_export_options_to_json(self):
        options = DatabaseSmugglerExportOptions(operate_on_types={DatabaseItemType.DOCUMENTS, DatabaseItemType.INDEXES})
        data = options.to_json()
        self.assertIn("OperateOnTypes", data)
        types = data["OperateOnTypes"]
        self.assertIn("Documents", types)
        self.assertIn("Indexes", types)

    def test_import_options_defaults(self):
        options = DatabaseSmugglerImportOptions()
        self.assertIsNotNone(options.operate_on_types)
        self.assertFalse(options.skip_revision_creation)

    def test_import_options_custom(self):
        options = DatabaseSmugglerImportOptions(
            operate_on_types={DatabaseItemType.DOCUMENTS},
            skip_revision_creation=True,
        )
        data = options.to_json()
        self.assertTrue(data["SkipRevisionCreation"])
        self.assertEqual(["Documents"], data["OperateOnTypes"])

    def test_import_options_to_json_has_required_keys(self):
        options = DatabaseSmugglerImportOptions()
        data = options.to_json()
        self.assertIn("OperateOnTypes", data)
        self.assertIn("SkipRevisionCreation", data)

    def test_database_smuggler_class_importable(self):
        self.assertIsNotNone(DatabaseSmuggler)
        self.assertIsNotNone(DatabaseSmugglerExportOptions)
        self.assertIsNotNone(DatabaseSmugglerImportOptions)

    def test_database_item_type_values(self):
        self.assertEqual("Documents", DatabaseItemType.DOCUMENTS.value)
        self.assertEqual("Indexes", DatabaseItemType.INDEXES.value)
        self.assertEqual("TimeSeries", DatabaseItemType.TIME_SERIES.value)
        self.assertEqual("TimeSeriesDeletedRanges", DatabaseItemType.TIME_SERIES_DELETED_RANGES.value)

    def test_options_defaults_match_csharp(self):
        opts = DatabaseSmugglerOptions()
        self.assertTrue(opts.include_expired)
        self.assertFalse(opts.include_artificial)
        self.assertTrue(opts.include_archived)
        self.assertFalse(opts.remove_analyzers)
        self.assertIsNone(opts.transform_script)
        self.assertEqual(10_000, opts.max_steps_for_transform_script)
        self.assertIsNone(opts.encryption_key)
        self.assertIsNone(opts.max_read_ops_per_second)
        self.assertFalse(opts.skip_corrupted_data)
        self.assertEqual([], opts.collections)

    def test_to_json_includes_all_base_fields(self):
        opts = DatabaseSmugglerImportOptions()
        data = opts.to_json()
        self.assertIn("OperateOnTypes", data)
        self.assertIn("OperateOnDatabaseRecordTypes", data)
        self.assertIn("IncludeExpired", data)
        self.assertIn("IncludeArtificial", data)
        self.assertIn("IncludeArchived", data)
        self.assertIn("RemoveAnalyzers", data)
        self.assertIn("MaxStepsForTransformScript", data)
        self.assertIn("SkipCorruptedData", data)
        self.assertIn("Collections", data)
        self.assertTrue(data["IncludeExpired"])
        self.assertFalse(data["IncludeArtificial"])
        self.assertTrue(data["IncludeArchived"])
        self.assertEqual(10_000, data["MaxStepsForTransformScript"])

    def test_optional_fields_omitted_when_none(self):
        opts = DatabaseSmugglerExportOptions()
        data = opts.to_json()
        self.assertNotIn("TransformScript", data)
        self.assertNotIn("EncryptionKey", data)
        self.assertNotIn("MaxReadOpsPerSecond", data)
        self.assertNotIn("CompressionAlgorithm", data)

    def test_optional_fields_present_when_set(self):
        opts = DatabaseSmugglerExportOptions(
            transform_script="this.Name = 'test';",
            encryption_key="secret",
            max_read_ops_per_second=100,
            compression_algorithm=ExportCompressionAlgorithm.ZSTD,
        )
        data = opts.to_json()
        self.assertEqual("this.Name = 'test';", data["TransformScript"])
        self.assertEqual("secret", data["EncryptionKey"])
        self.assertEqual(100, data["MaxReadOpsPerSecond"])
        self.assertEqual("Zstd", data["CompressionAlgorithm"])

    def test_operate_on_database_record_types_defaults(self):
        opts = DatabaseSmugglerOptions()
        self.assertIn(DatabaseRecordItemType.CLIENT, opts.operate_on_database_record_types)
        self.assertIn(DatabaseRecordItemType.REVISIONS, opts.operate_on_database_record_types)
        self.assertIn(DatabaseRecordItemType.INDEXES_HISTORY, opts.operate_on_database_record_types)
        self.assertIn(DatabaseRecordItemType.SCHEMA_VALIDATION, opts.operate_on_database_record_types)

    def test_operate_on_database_record_types_serialized(self):
        opts = DatabaseSmugglerImportOptions(
            operate_on_database_record_types={DatabaseRecordItemType.REVISIONS, DatabaseRecordItemType.SETTINGS}
        )
        data = opts.to_json()
        record_types = data["OperateOnDatabaseRecordTypes"]
        self.assertIn("Revisions", record_types)
        self.assertIn("Settings", record_types)
        self.assertEqual(2, len(record_types))

    def test_for_database_returns_new_instance_for_different_name(self):
        class _MockStore:
            database = "testdb"

        smuggler = DatabaseSmuggler(_MockStore(), "testdb")
        other = smuggler.for_database("other-db")
        self.assertIsInstance(other, DatabaseSmuggler)

    def test_for_database_same_name_returns_self(self):
        class _MockStore:
            database = "testdb"

        smuggler = DatabaseSmuggler(_MockStore(), "testdb")
        self.assertIs(smuggler, smuggler.for_database("testdb"))
        self.assertIs(smuggler, smuggler.for_database("TESTDB"))
        self.assertIs(smuggler, smuggler.for_database("TestDB"))


class TestDatabaseSmugglerIntegration(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_store_has_smuggler_property(self):
        smuggler = self.store.smuggler
        self.assertIsInstance(smuggler, DatabaseSmuggler)

    def test_smuggler_property_is_cached(self):
        self.assertIs(self.store.smuggler, self.store.smuggler)


if __name__ == "__main__":
    unittest.main()
