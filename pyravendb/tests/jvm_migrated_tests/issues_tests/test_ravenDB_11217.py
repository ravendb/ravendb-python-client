from typing import Optional

from pyravendb.documents.session.misc import SessionOptions
from pyravendb.documents.session.query import QueryStatistics
from pyravendb.infrastructure.orders import Product
from pyravendb.tests.test_base import TestBase


class TestRavenDB11217(TestBase):
    def setUp(self):
        super(TestRavenDB11217, self).setUp()

    def test_session_wide_no_caching_should_work(self):
        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
            self.assertGreaterEqual(stats.duration_in_ms, 0)
            list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
            self.assertEqual(-1, stats.duration_in_ms)

        no_cache_options = SessionOptions()
        no_cache_options.no_caching = True

        with self.store.open_session(session_options=no_cache_options) as session:
            list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
            self.assertGreaterEqual(stats.duration_in_ms, 0)
            list(session.query(object_type=Product).statistics(__stats_callback).where_equals("name", "HR"))
            self.assertGreaterEqual(stats.duration_in_ms, 0)
