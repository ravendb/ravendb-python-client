from typing import Optional

from ravendb.documents.session.stream_statistics import StreamQueryStatistics
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB14109(TestBase):
    def setUp(self):
        super(TestRavenDB14109, self).setUp()

    def test_query_stats_should_be_filled_before_calling_move_next(self):
        with self.store.open_session() as session:
            session.store(Company())
            session.store(Company())

            session.save_changes()

        with self.store.open_session() as session:
            stats: Optional[StreamQueryStatistics] = None
            self.callback_ran = False

            def __stats_callback(statistics: StreamQueryStatistics):
                nonlocal stats
                stats = statistics
                self.callback_ran = True

            query = session.query(object_type=Company)

            iterator = session.advanced.stream_with_statistics(query, __stats_callback)
            self.assertEqual(2, stats.total_results)

            count = 0
            for i in iterator:
                count += 1

            self.assertEqual(count, stats.total_results)
            self.assertTrue(self.callback_ran)
