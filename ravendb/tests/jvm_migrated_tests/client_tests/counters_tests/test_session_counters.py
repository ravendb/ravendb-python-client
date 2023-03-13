from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.tests.test_base import TestBase, User


class TestSessionCounters(TestBase):
    def setUp(self):
        super(TestSessionCounters, self).setUp()

    def test_session_should_keep_nulls_in_counters_cache(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1-A").get("score")
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertIsNone(val)

            val = session.counters_for("users/1-A").get("score")
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertIsNone(val)

            dic = session.counters_for("users/1-A").get_all()
            # should not contain None value for "score"

            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

    def test_session_should_remove_counters_from_cache_after_document_deletion(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.save_changes()

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_many(["likes", "dislikes"])

            self.assertEqual(2, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertEqual(1, session.advanced.number_of_requests)

            session.delete("users/1-A")
            session.save_changes()

            self.assertEqual(2, session.advanced.number_of_requests)

            val = session.counters_for("users/1-A").get("likes")
            self.assertIsNone(val)

            self.assertEqual(3, session.advanced.number_of_requests)

    def test_session_delete_counter(self):
        with self.store.open_session() as session:
            user1 = User("Aviv1")
            user2 = User("Aviv2")
            session.store(user1, "users/1-A")
            session.store(user2, "users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("downloads", 500)
            session.counters_for("users/2-A").increment("votes", 1000)

            session.save_changes()

        counters = self.store.operations.send(GetCountersOperation("users/1-A", ["likes", "downloads"])).counters
        self.assertEqual(2, len(counters))

        with self.store.open_session() as session:
            session.counters_for("users/1-A").delete("likes")
            session.counters_for("users/1-A").delete("downloads")
            session.counters_for("users/2-A").delete("votes")

            session.save_changes()

        counters = self.store.operations.send(GetCountersOperation("users/1-A", ["likes", "downloads"])).counters

        self.assertEqual(2, len(counters))
        self.assertIsNone(counters[0])
        self.assertIsNone(counters[1])

        counters = self.store.operations.send(GetCountersOperation("users/2-A", ["votes"])).counters
        self.assertEqual(1, len(counters))
        self.assertIsNone(counters[0])

    def test_session_should_always_loads_counters_from_cache_after_get_all(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)
            session.save_changes()

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_all()
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            # should not go to server after get_all request
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(100, val)

            val = session.counters_for("users/1-A").get("votes")
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertIsNone(val)

    def test_different_types_of_counters_operations_in_one_session(self):
        with self.store.open_session() as session:
            user1 = User("Aviv1")
            user2 = User("Aviv2")
            session.store(user1, "users/1-A")
            session.store(user2, "users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/2-A").increment("votes", 1000)

            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").delete("downloads")
            session.counters_for("users/2-A").increment("votes", -600)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(200, val)

            val = session.counters_for("users/1-A").get("downloads")
            self.assertIsNone(val)

            val = session.counters_for("users/2-A").get("votes")
            self.assertEqual(400, val)
