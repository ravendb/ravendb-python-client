import uuid

from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import CreateDatabaseOperation, DeleteDatabaseOperation
from ravendb.documents.operations.counters import (
    GetCountersOperation,
    DocumentCountersOperation,
    CounterOperation,
    CounterOperationType,
    CounterBatch,
    CounterBatchOperation,
)
from ravendb.tests.test_base import TestBase, User, Company, Order


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

    def test_session_get_counters_with_non_default_database(self):
        db_name = f"db-{uuid.uuid4().__str__()}"
        self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(db_name)))
        try:
            with self.store.open_session(db_name) as session:
                user1 = User("Aviv1")
                user2 = User("Aviv2")
                session.store(user1, "users/1-A")
                session.store(user2, "users/2-A")
                session.save_changes()

            with self.store.open_session(db_name) as session:
                session.counters_for("users/1-A").increment("likes", 100)
                session.counters_for("users/1-A").increment("downloads", 500)
                session.counters_for("users/2-A").increment("votes", 1000)

                session.save_changes()

            with self.store.open_session(db_name) as session:
                dic = session.counters_for("users/1-A").get_all()

                self.assertEqual(2, len(dic))
                self.assertIn(("likes", 100), dic.items())
                self.assertIn(("downloads", 500), dic.items())

                x = session.counters_for("users/2-A").get_all()
                val = session.counters_for("users/2-A").get("votes")
                self.assertEqual(1000, val)
        finally:
            self.store.maintenance.server.send(DeleteDatabaseOperation(db_name, True))

    def test_should_throw(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            session.counters_for_entity(user).increment("likes", 100)
            session.save_changes()

            self.assertEqual(100, session.counters_for(user).get("likes"))

    def test_should_track_counters(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.number_of_requests)
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(100, val)
            self.assertEqual(1, session.advanced.number_of_requests)

            session.counters_for("users/1-A").get("likes")
            self.assertEqual(1, session.advanced.number_of_requests)

    def test_session_clear_should_clear_counters_cache(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            user_counters = session.counters_for("users/1-A")
            dic = user_counters.get_all()
            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            with self.store.open_session() as session2:
                session2.counters_for("users/1-A").increment("likes")
                session2.counters_for("users/1-A").delete("dislikes")
                session2.counters_for("users/1-A").increment("score", 1000)  # new counter
                session2.save_changes()

            session.advanced.clear()  # should clear counters cache

            dic = user_counters.get_all()  # should go ot server again
            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 101), dic.items())
            self.assertIn(("downloads", 300), dic.items())
            self.assertIn(("score", 1000), dic.items())

    def test_session_get_counters(self):
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

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_all()

            self.assertEqual(2, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("downloads", 500), dic.items())

            val = session.counters_for("users/2-A").get("votes")
            self.assertEqual(1000, val)

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_many(["likes", "downloads"])
            self.assertEqual(2, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("downloads", 500), dic.items())

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_many(["likes"])
            self.assertEqual(1, len(dic))
            self.assertIn(("likes", 100), dic.items())

    def test_session_should_update_missing_counters_in_cache_and_remove_deleted_counters__after_refresh(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            self.assertEqual(1, session.advanced.number_of_requests)

            user_counters = session.counters_for_entity(user)
            dic = user_counters.get_all()
            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            with self.store.open_session() as session2:
                session2.counters_for("users/1-A").increment("likes")
                session2.counters_for("users/1-A").delete("dislikes")
                session2.counters_for("users/1-A").increment("score", 1000)  # new counter
                session2.save_changes()

            session.advanced.refresh(user)

            self.assertEqual(3, session.advanced.number_of_requests)

            # Refresh updated the document in session,
            # cache should know that it's missing 'score' by looking
            # at the document's metadata and go to server again to get all.
            # this should override the cache entirely and therefore
            # 'dislikes' won't be in cache anymore

            dic = user_counters.get_all()
            self.assertEqual(4, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 101), dic.items())
            self.assertIn(("downloads", 300), dic.items())
            self.assertIn(("score", 1000), dic.items())

            # cache should know that it got all and not go to server,
            # and it shouldn't have 'dislikes' entry anymore

            val = user_counters.get("dislikes")
            self.assertEqual(4, session.advanced.number_of_requests)
            self.assertIsNone(val)

    def test_session_should_remove_counter_from_cache_after_counter_deletion(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(100, val)
            self.assertEqual(1, session.advanced.number_of_requests)

            session.counters_for("users/1-A").delete("likes")
            session.save_changes()
            self.assertEqual(2, session.advanced.number_of_requests)

            val = session.counters_for("users/1-A").get("likes")
            self.assertIsNone(val)
            self.assertEqual(3, session.advanced.number_of_requests)

    def test_session_increment_counter(self):
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
        self.assertEqual(100, list(filter(lambda x: x.counter_name == "likes", counters))[0].total_value)
        self.assertEqual(500, list(filter(lambda x: x.counter_name == "downloads", counters))[0].total_value)

        counters = self.store.operations.send(GetCountersOperation("users/2-A", ["votes"])).counters
        self.assertEqual(1, len(counters))
        self.assertEqual(1000, list(filter(lambda x: x.counter_name == "votes", counters))[0].total_value)

    def test_session_should_update_missing_counters_in_cache_and_remove_deleted_counters__after_load_from_server(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            user_counters = session.counters_for("users/1-A")
            dic = user_counters.get_all()
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            with self.store.open_session() as session2:
                session2.counters_for("users/1-A").increment("likes")
                session2.counters_for("users/1-A").delete("dislikes")
                session2.counters_for("users/1-A").increment("score", 1000)  # new counter
                session2.save_changes()

            user = session.load("users/1-A", User)

            self.assertEqual(2, session.advanced.number_of_requests)

            # Refresh updated the document in session,
            # cache should know that it's missing 'score' by looking
            # at the document's metadata and go to server again to get all.
            # this should override the cache entirely and therefore
            # 'dislikes' won't be in cache anymore

            dic = user_counters.get_all()
            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 101), dic.items())
            self.assertIn(("downloads", 300), dic.items())
            self.assertIn(("score", 1000), dic.items())

            # cache should know that it got all and not go to server,
            # and it shouldn't have 'dislikes' entry anymore

            val = user_counters.get("dislikes")
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertIsNone(val)

    def test_session_increment_counter_should_update_counter_value_after_save_changes(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 300)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(100, val)
            self.assertEqual(1, session.advanced.number_of_requests)

            session.counters_for("users/1-A").increment("likes", 50)  # should not increment the counter value in cache
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(val, 100)
            self.assertEqual(1, session.advanced.number_of_requests)

            session.counters_for("users/1-A").increment("dislikes", 200)  # should not add the counter to cache
            val = session.counters_for("users/1-A").get("dislikes")  # should go to server
            self.assertEqual(300, val)
            self.assertEqual(2, session.advanced.number_of_requests)

            session.counters_for("users/1-A").increment("score", 1000)  # should not add the counter to cache
            self.assertEqual(2, session.advanced.number_of_requests)

            # save_changes should updated counters values in cache
            # according to increment result
            session.save_changes()
            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server for these
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(150, val)

            val = session.counters_for("users/1-A").get("dislikes")
            self.assertEqual(500, val)

            val = session.counters_for("users/1-A").get("score")
            self.assertEqual(1000, val)

            self.assertEqual(3, session.advanced.number_of_requests)

    def test_session_should_know_when_it_has_all_counters_in_cache_and_avoid_trip_to_server__when_using_entity(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            self.assertEqual(1, session.advanced.number_of_requests)

            user_counters = session.counters_for_entity(user)

            val = user_counters.get("likes")
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(100, val)

            val = user_counters.get("dislikes")
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertEqual(200, val)

            val = user_counters.get("downloads")
            # session should know at this point that it has all counters

            self.assertEqual(4, session.advanced.number_of_requests)
            self.assertEqual(300, val)

            dic = user_counters.get_all()  # should not go to server
            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            val = user_counters.get("score")  # should not go to server
            self.assertEqual(4, session.advanced.number_of_requests)
            self.assertIsNone(val)

    def test_session_evict_should_remove_entry_from_counters_cache(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            self.assertEqual(1, session.advanced.number_of_requests)

            user_counters = session.counters_for("users/1-A")
            dic = user_counters.get_all()
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 100), dic.items())
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())

            with self.store.open_session() as session2:
                session2.counters_for("users/1-A").increment("likes")
                session2.counters_for("users/1-A").delete("dislikes")
                session2.counters_for("users/1-A").increment("score", 1000)  # new counter
                session2.save_changes()

            session.advanced.evict(user)  # should remove 'users/1-A' entry from CountersByDocId

            dic = user_counters.get_all()
            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(3, len(dic))
            self.assertIn(("likes", 101), dic.items())
            self.assertIn(("downloads", 300), dic.items())
            self.assertIn(("score", 1000), dic.items())

    def test_session_should_override_existing_counter_values_in_cache_after_get_all(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1-A")
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("dislikes", 200)
            session.counters_for("users/1-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1-A").get("likes")
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(100, val)

            val = session.counters_for("users/1-A").get("score")
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertIsNone(val)

            operation = DocumentCountersOperation(
                "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 400)]
            )

            counter_batch = CounterBatch(documents=[operation])
            self.store.operations.send(CounterBatchOperation(counter_batch))

            dic = session.counters_for("users/1-A").get_all()
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertEqual(3, len(dic))
            self.assertIn(("dislikes", 200), dic.items())
            self.assertIn(("downloads", 300), dic.items())
            self.assertIn(("likes", 500), dic.items())

            val = session.counters_for("users/1-A").get("score")

            self.assertEqual(3, session.advanced.number_of_requests)  # null values should be in cache
            self.assertIsNone(val)

    def test_get_counters_for(self):
        with self.store.open_session() as session:
            user1 = User("Aviv1")
            session.store(user1, "users/1-A")

            user2 = User("Aviv2")
            session.store(user2, "users/2-A")

            user3 = User("Aviv3")
            session.store(user3, "users/3-A")

            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1-A").increment("likes", 100)
            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/2-A").increment("votes", 1000)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            counters = session.advanced.get_counters_for(user)

            self.assertEqual(2, len(counters))
            self.assertSequenceContainsElements(counters, "downloads", "likes")

            user = session.load("users/2-A", User)
            counters = session.advanced.get_counters_for(user)

            self.assertEqual(1, len(counters))
            self.assertEqual(["votes"], counters)

            user = session.load("users/3-A", User)
            counters = session.advanced.get_counters_for(user)
            self.assertIsNone(counters)

    def test_session_include_single_counter_after_include_all_counters_should_throw(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1-A")
            order = Order(company="companies/1-A")
            session.store(order, "orders/1-A")

            session.counters_for("orders/1-A").increment("likes", 100)
            session.counters_for("orders/1-A").increment("dislikes", 200)
            session.counters_for("orders/1-A").increment("downloads", 300)
            session.save_changes()

        with self.store.open_session() as session:
            with self.assertRaises(RuntimeError):
                session.load(
                    "orders/1-A",
                    Order,
                    lambda i: i.include_documents("company").include_all_counters().include_counter("likes"),
                )
