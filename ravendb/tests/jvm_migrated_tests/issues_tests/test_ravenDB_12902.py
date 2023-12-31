from typing import Optional

from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
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

    class UsersByName(AbstractIndexCreationTask):
        def __init__(self):
            super().__init__()
            self.map = (
                "from u in docs.Users select new " " {" "    firstNAme = u.name, " "    lastName = u.lastName" "}"
            )

            super()._suggestion("name")

    def test_after_lazy_suggestion_query_executed_should_be_executed_only_once(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())
        with self.store.open_session() as session:
            counter = 0
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            def __after_query_executed_callback(query_result: QueryResult):
                nonlocal counter
                counter += 1

            results = (
                session.query_index_type(TestRavenDB12902.UsersByName, User)
                .add_after_query_executed_listener(__after_query_executed_callback)
                .statistics(__stats_callback)
                .suggest_using(lambda x: x.by_field("name", "Orin"))
                .execute_lazy()
                .value
            )

            self.assertEqual(1, len(results))
            self.assertEqual(0, len(results.get("name").suggestions))
            self.assertIsNotNone(stats)
            self.assertEqual(1, counter)

    def test_after_lazy_aggregation_query_executed_should_be_executed_only_once(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())

        with self.store.open_session() as session:
            counter = 0
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            def __after_query_executed_callback(query_result: QueryResult):
                nonlocal counter
                counter += 1

            result = (
                session.query_index_type(TestRavenDB12902.UsersByName, User)
                .statistics(__stats_callback)
                .add_after_query_executed_listener(__after_query_executed_callback)
                .where_equals("name", "Doe")
                .aggregate_by(lambda x: x.by_field("name").sum_on("count"))
                .execute_lazy()
                .value
            )

    def test_can_get_valid_statistics_in_suggestion_query(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())

        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            session.query_index_type(TestRavenDB12902.UsersByName, User).statistics(__stats_callback).suggest_using(
                lambda x: x.by_field("name", "Orin")
            ).execute()

            self.assertIsNotNone(stats.index_name)

    def test_after_suggestion_query_executed_should_be_executed_only_once(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())
        with self.store.open_session() as session:
            counter = 0
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            def __after_query_executed_callback(query_result: QueryResult):
                nonlocal counter
                counter += 1

            results = (
                session.query_index_type(TestRavenDB12902.UsersByName, User)
                .add_after_query_executed_listener(__after_query_executed_callback)
                .statistics(__stats_callback)
                .suggest_using(lambda x: x.by_field("name", "Orin"))
                .execute()
            )

            self.assertEqual(1, len(results))
            self.assertEqual(0, len(results.get("name").suggestions))
            self.assertIsNotNone(stats)
            self.assertEqual(1, counter)

    def test_after_aggregation_query_executed_should_be_executed_only_once(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())

        with self.store.open_session() as session:
            counter = 0
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            def __after_query_executed_callback(query_result: QueryResult):
                nonlocal counter
                counter += 1

            result = (
                session.query_index_type(TestRavenDB12902.UsersByName, User)
                .statistics(__stats_callback)
                .add_after_query_executed_listener(__after_query_executed_callback)
                .where_equals("name", "Doe")
                .aggregate_by(lambda x: x.by_field("name").sum_on("count"))
                .execute()
            )

            self.assertEqual(1, len(result))
            self.assertEqual(0, len(result["name"].values))
            self.assertIsNotNone(stats)
            self.assertEqual(1, counter)

    def test_can_get_valid_statistics_in_aggregation_query(self):
        self.store.execute_index(TestRavenDB12902.UsersByName())

        with self.store.open_session() as session:
            stats: Optional[QueryStatistics] = None

            def __stats_callback(statistics: QueryStatistics):
                nonlocal stats
                stats = statistics

            session.query_index_type(TestRavenDB12902.UsersByName, User).statistics(__stats_callback).where_equals(
                "name", "Doe"
            ).aggregate_by(lambda x: x.by_field("name").sum_on("count")).execute()
            self.assertIsNotNone(stats.index_name)
