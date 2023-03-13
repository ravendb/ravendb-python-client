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
