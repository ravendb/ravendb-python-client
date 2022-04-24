from typing import Optional

from ravendb.documents.queries.query import QueryResult
from ravendb.documents.session.query import QueryStatistics
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB12902(TestBase):
    def setUp(self):
        super(TestRavenDB12902, self).setUp()

    def test_after_lazy_query_executed_should_be_executed_only_once(self):
        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            def __after_query_executed_callback(query_result: QueryResult):
                nonlocal counter
                counter += 1

            counter = 0
            results = (
                session.query(object_type=User)
                .add_after_query_executed_listener(__after_query_executed_callback)
                .statistics(__stats_callback)
                .where_equals("name", "Doe")
                .lazily()
                .value
            )

            self.assertEqual(0, len(results))
            self.assertIsNotNone(stats)
            self.assertEqual(1, counter)
