from datetime import datetime

from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.definitions import FieldIndexing, FieldStorage
from ravendb.documents.session.stream_statistics import StreamQueryStatistics
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class Users_ByName(AbstractIndexCreationTask):
    def __init__(self):
        super(Users_ByName, self).__init__()
        self.map = "from u in docs.Users select new { u.name, last_name = u.last_name }"
        self._index("name", FieldIndexing.SEARCH)
        self._index_suggestions.add("name")
        self._store("name", FieldStorage.YES)


class TestQueryStreaming(TestBase):
    def setUp(self):
        super(TestQueryStreaming, self).setUp()

    def test_can_stream_query_results(self):
        Users_ByName().execute(self.store)

        with self.store.open_session() as session:
            for i in range(200):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        count = 0

        with self.store.open_session() as session:
            query = session.query_index_type(Users_ByName, User)
            stream = session.advanced.stream(query)
            for user in stream:
                count += 1
                self.assertIsNotNone(user)

        self.assertEqual(200, count)

    def test_can_stream_raw_query_results(self):
        Users_ByName().execute(self.store)

        with self.store.open_session() as session:
            for i in range(200):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        count = 0

        with self.store.open_session() as session:
            query = session.advanced.raw_query(f"from index '{Users_ByName().index_name}'")
            stream = session.advanced.stream(query)
            for user in stream:
                count += 1
                self.assertIsNotNone(user)

        self.assertEqual(200, count)

    def test_can_stream_query_results_with_query_statistics(self):
        Users_ByName().execute(self.store)
        self.callback_ran = False

        with self.store.open_session() as session:
            for i in range(100):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:

            def __stats_callback(statistics: StreamQueryStatistics):
                self.assertEqual("Users/ByName", statistics.index_name)
                self.assertEqual(100, statistics.total_results)
                self.assertEqual(datetime.now().year, statistics.index_timestamp.year)
                self.callback_ran = True

            query = session.query_index_type(Users_ByName, User)
            stream = session.advanced.stream_with_statistics(query, __stats_callback)
            for user in stream:
                self.assertIsNotNone(user)

            self.assertTrue(self.callback_ran)

    def test_can_stream_raw_query_results_with_query_statistics(self):
        Users_ByName().execute(self.store)
        self.callback_ran = False

        with self.store.open_session() as session:
            for i in range(100):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:

            def __stats_callback(statistics: StreamQueryStatistics):
                self.assertEqual("Users/ByName", statistics.index_name)
                self.assertEqual(100, statistics.total_results)
                self.assertEqual(datetime.now().year, statistics.index_timestamp.year)
                self.callback_ran = True

            query = session.advanced.raw_query(f"from index '{Users_ByName().index_name}'")
            stream = session.advanced.stream_with_statistics(query, __stats_callback)
            for user in stream:
                self.assertIsNotNone(user)

            self.assertTrue(self.callback_ran)
