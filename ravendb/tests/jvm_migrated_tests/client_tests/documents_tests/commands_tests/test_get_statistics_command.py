from datetime import datetime, timedelta

from ravendb import GetStatisticsOperation
from ravendb.documents.smuggler.common import DatabaseItemType
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.operations import CreateSampleDataOperation
from ravendb.tests.test_base import TestBase


class TestGetStatisticsCommand(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_stats(self):
        executor = self.store.get_request_executor()

        sample_data = CreateSampleDataOperation(
            {
                DatabaseItemType.DOCUMENTS,
                DatabaseItemType.INDEXES,
                DatabaseItemType.ATTACHMENTS,
                DatabaseItemType.REVISION_DOCUMENTS,
            }
        )

        self.store.maintenance.send(sample_data)

        self.wait_for_indexing(self.store)

        command = GetStatisticsOperation._GetStatisticsCommand()
        executor.execute_command(command)

        stats = command.result
        self.assertIsNotNone(stats)

        self.assertIsNotNone(stats.last_doc_etag)
        self.assertGreater(stats.last_doc_etag, 0)

        self.assertGreaterEqual(stats.count_of_indexes, 3)
        self.assertGreaterEqual(1059, stats.count_of_documents)

        self.assertGreater(stats.count_of_revision_documents, 0)

        self.assertGreaterEqual(0, stats.count_of_documents_conflicts)

        self.assertGreaterEqual(0, stats.count_of_conflicts)

        self.assertGreaterEqual(17, stats.count_of_unique_attachments)

        self.assertEqual(17, stats.count_of_unique_attachments)

        self.assertIsNotNone(stats.database_change_vector)

        self.assertIsNotNone(stats.database_id)

        self.assertIsNotNone(stats.pager)

        self.assertIsNotNone(stats.last_indexing_time)

        self.assertIsNotNone(stats.indexes)

        self.assertIsNotNone(stats.size_on_disk.human_size)

        self.assertIsNotNone(stats.size_on_disk.size_in_bytes)

        for index_information in stats.indexes:
            self.assertIsNotNone(index_information.name)

            self.assertFalse(index_information.stale)

            self.assertIsNotNone(index_information.state)

            self.assertIsNotNone(index_information.lock_mode)

            self.assertIsNotNone(index_information.priority)

            self.assertIsNotNone(index_information.type)

            self.assertIsNotNone(index_information.last_indexing_time)

    def test_can_get_stats_for_counters_and_time_series(self):
        with self.store.open_session() as session:
            session.store(User(), "users/1")
            session.counters_for("users/1").increment("c1")
            session.counters_for("users/1").increment("c2")
            tsf = session.time_series_for("users/1", "Heartrate")
            tsf.append(datetime.now(), [70])
            tsf.append(datetime.now() + timedelta(minutes=1), [20])
            session.save_changes()

        db_statistics = self.store.maintenance.send(GetStatisticsOperation())
        self.assertEqual(1, db_statistics.count_of_counter_entries)
        self.assertEqual(1, db_statistics.count_of_time_series_segments)
