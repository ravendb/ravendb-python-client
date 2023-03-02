from datetime import datetime

from ravendb import AbstractIndexCreationTask
from ravendb.documents.indexes.definitions import FieldIndexing, FieldStorage
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class Users_ByName(AbstractIndexCreationTask):
    def __init__(self):
        super(Users_ByName, self).__init__()
        self.map = "from u in docs.Users select new { u.name, lastName = u.lastName.Boost(10) }"
        self._index("name", FieldIndexing.SEARCH)
        self._index_suggestions.update("name")
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

        with self.store.open_session() as session:
            for i in range(100):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query = session.query_index_type(Users_ByName, User)
            stream, stats = session.advanced.stream_with_statistics(query)
            for user in stream:
                self.assertIsNotNone(user)

            self.assertEqual("Users/ByName", stats.index_name)
            self.assertEqual(100, stats.total_results)
            self.assertEqual(datetime.now().year, stats.index_timestamp.year)

    def test_can_stream_raw_query_results_with_query_statistics(self):
        Users_ByName().execute(self.store)

        with self.store.open_session() as session:
            for i in range(100):
                session.store(User())

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query = session.advanced.raw_query(f"from index '{Users_ByName().index_name}'")
            stream, stats = session.advanced.stream_with_statistics(query)
            for user in stream:
                self.assertIsNotNone(user)

            self.assertEqual("Users/ByName", stats.index_name)
            self.assertEqual(100, stats.total_results)
            self.assertEqual(datetime.now().year, stats.index_timestamp.year)
