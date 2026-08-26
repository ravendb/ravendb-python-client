"""
RDBC-1033: GetEssentialStatisticsOperation hits /stats/essential.

C# reference: SlowTests.Issues/Issues/RavenDB_18648.cs
              Can_Get_Essential_Database_Statistics()
"""

import unittest

from ravendb.documents.indexes.definitions import (
    ArchivedDataProcessingBehavior,
    IndexLockMode,
    IndexPriority,
    IndexType,
    IndexSourceType,
)
from ravendb.documents.operations.statistics import (
    EssentialDatabaseStatistics,
    EssentialIndexInformation,
    GetEssentialStatisticsOperation,
)
from ravendb.tests.test_base import TestBase


class TestEssentialStatisticsUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_essential_database_statistics_from_json(self):
        stats = EssentialDatabaseStatistics.from_json(
            {
                "CountOfIndexes": 5,
                "CountOfDocuments": 42,
                "CountOfRevisionDocuments": 10,
                "CountOfDocumentsConflicts": 1,
                "CountOfTombstones": 3,
                "CountOfConflicts": 1,
                "CountOfAttachments": 3,
                "CountOfCounterEntries": 7,
                "CountOfTimeSeriesSegments": 2,
            }
        )
        self.assertEqual(5, stats.count_of_indexes)
        self.assertEqual(42, stats.count_of_documents)
        self.assertEqual(10, stats.count_of_revision_documents)
        self.assertEqual(1, stats.count_of_documents_conflicts)
        self.assertEqual(3, stats.count_of_tombstones)
        self.assertEqual(1, stats.count_of_conflicts)
        self.assertEqual(3, stats.count_of_attachments)
        self.assertEqual(7, stats.count_of_counter_entries)
        self.assertEqual(2, stats.count_of_time_series_segments)
        self.assertIsNone(stats.indexes)

    def test_essential_statistics_empty_json(self):
        stats = EssentialDatabaseStatistics.from_json({})
        self.assertIsNone(stats.count_of_documents)
        self.assertIsNone(stats.count_of_indexes)
        self.assertIsNone(stats.count_of_revision_documents)
        self.assertIsNone(stats.count_of_tombstones)
        self.assertIsNone(stats.indexes)

    def test_essential_statistics_with_indexes(self):
        stats = EssentialDatabaseStatistics.from_json(
            {
                "CountOfIndexes": 1,
                "CountOfDocuments": 0,
                "Indexes": [
                    {
                        "Name": "Orders/ByCompany",
                        "LockMode": "Unlock",
                        "Priority": "Normal",
                        "Type": "Map",
                        "SourceType": "Documents",
                    }
                ],
            }
        )
        self.assertIsNotNone(stats.indexes)
        self.assertEqual(1, len(stats.indexes))
        idx = stats.indexes[0]
        self.assertEqual("Orders/ByCompany", idx.name)
        self.assertEqual(IndexLockMode.UNLOCK, idx.lock_mode)
        self.assertEqual(IndexPriority.NORMAL, idx.priority)
        self.assertEqual(IndexType.MAP, idx.type)
        self.assertEqual(IndexSourceType.DOCUMENTS, idx.source_type)
        self.assertIsNone(idx.archived_data_processing_behavior)

    def test_essential_index_information_archived_behavior(self):
        idx = EssentialIndexInformation.from_json(
            {
                "Name": "idx",
                "LockMode": "Unlock",
                "Priority": "Normal",
                "Type": "Map",
                "SourceType": "Documents",
                "ArchivedDataProcessingBehavior": "IncludeArchived",
            }
        )
        self.assertEqual(ArchivedDataProcessingBehavior.INCLUDE_ARCHIVED, idx.archived_data_processing_behavior)

    def test_essential_statistics_empty_indexes_list(self):
        stats = EssentialDatabaseStatistics.from_json({"CountOfIndexes": 0, "Indexes": []})
        self.assertIsNotNone(stats.indexes)
        self.assertEqual([], stats.indexes)

    def test_get_essential_statistics_operation_importable(self):
        op = GetEssentialStatisticsOperation()
        self.assertIsNotNone(op)

    def test_get_essential_statistics_url_contains_debug_tag(self):
        from ravendb.http.server_node import ServerNode
        from ravendb.documents.conventions import DocumentConventions

        node = ServerNode("http://localhost:8080", "testdb")
        op = GetEssentialStatisticsOperation(debug_tag="src=test")
        cmd = op.get_command(DocumentConventions())
        req = cmd.create_request(node)
        self.assertIn("/stats/essential", req.url)
        self.assertIn("src=test", req.url)

    def test_get_essential_statistics_url_no_debug_tag(self):
        from ravendb.http.server_node import ServerNode
        from ravendb.documents.conventions import DocumentConventions

        node = ServerNode("http://localhost:8080", "testdb")
        op = GetEssentialStatisticsOperation()
        cmd = op.get_command(DocumentConventions())
        req = cmd.create_request(node)
        self.assertEqual("http://localhost:8080/databases/testdb/stats/essential", req.url)

    def test_top_level_package_exports(self):
        import ravendb

        self.assertTrue(hasattr(ravendb, "GetEssentialStatisticsOperation"))
        self.assertTrue(hasattr(ravendb, "EssentialDatabaseStatistics"))
        self.assertTrue(hasattr(ravendb, "EssentialIndexInformation"))
        self.assertTrue(hasattr(ravendb, "ArchivedDataProcessingBehavior"))


class TestEssentialStatistics(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_get_essential_statistics_operation_returns_result(self):
        from ravendb.infrastructure.orders import Product

        with self.store.open_session() as session:
            p = Product()
            p.name = "Widget"
            session.store(p, "products/1")
            session.save_changes()

        stats = self.store.maintenance.send(GetEssentialStatisticsOperation())

        self.assertIsInstance(stats, EssentialDatabaseStatistics)
        self.assertGreaterEqual(stats.count_of_documents, 1)
        self.assertIsNotNone(stats.count_of_indexes)

    def test_get_essential_statistics_empty_database(self):
        stats = self.store.maintenance.send(GetEssentialStatisticsOperation())
        self.assertIsInstance(stats, EssentialDatabaseStatistics)
        self.assertEqual(0, stats.count_of_documents)
        self.assertEqual(0, stats.count_of_tombstones)
        self.assertEqual(0, stats.count_of_conflicts)


if __name__ == "__main__":
    unittest.main()
