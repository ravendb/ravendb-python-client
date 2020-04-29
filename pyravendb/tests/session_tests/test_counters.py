from pyravendb.tests.test_base import TestBase
import unittest


class User:
    def __init__(self, doc_id, name):
        self.Id = doc_id
        self.name = name


class TestCounters(TestBase):
    def setUp(self):
        super().setUp()

        with self.store.open_session() as session:
            session.store(User("users/1-A", "Idan"))
            session.save_changes()

    def tearDown(self):
        super().tearDown()
        self.delete_all_topology_files()

    def test_counter_increment(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=1)
            session.save_changes()

            counters = document_counter.get_all()
            self.assertEqual(len(counters), 2)
            self.assertTrue(counters["Likes"] == 10 and counters["Shares"] == 1)

            document_counter.increment("Likes", delta=20)
            session.save_changes()
            counters = document_counter.get_all()
            self.assertTrue(counters["Likes"] == 30)

    def test_counters_delete(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            session.save_changes()
            counters = document_counter.get_all()
            self.assertEqual(len(counters), 1)

        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.delete("Likes")
            session.save_changes()

            counters = document_counter.get_all()
            self.assertTrue(not counters)

    def test_counters_cache(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=122)
            session.save_changes()

        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.get("Shares")
            document_counter.get_all()

            self.assertEqual(session.advanced.number_of_requests_in_session(), 2)
            document_counter.get("Likes")
            self.assertEqual(session.advanced.number_of_requests_in_session(), 2)


if __name__ == '__main__':
    unittest.main()
