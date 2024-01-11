from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestBulkInsertCounters(TestBase):
    def setUp(self):
        super().setUp()

    def test_add_document_after_increment_counter(self):
        with self.store.bulk_insert() as bulk_insert:
            user1 = User()
            user1.name = "Grisha"
            bulk_insert.store(user1)

            user_id_1 = user1.Id

        with self.store.bulk_insert() as bulk_insert:
            counter = bulk_insert.counters_for(user_id_1)

            counter.increment("likes", 100)
            counter.increment("downloads", 500)

            user2 = User(name="Kotler")
            bulk_insert.store(user2)

            user_id_2 = user2.Id

            bulk_insert.counters_for(user_id_2).increment("votes", 1000)

        counters = self.store.operations.send(GetCountersOperation(user_id_1, ["likes", "downloads"])).counters

        self.assertEqual(2, len(counters))

        counters.sort(key=lambda counter: counter.counter_name)

        self.assertEqual(500, counters[0].total_value)

        self.assertEqual(100, counters[1].total_value)

        val = self.store.operations.send(GetCountersOperation(user_id_2, "votes"))

        self.assertEqual(1000, val.counters[0].total_value)

    def test_increment_counter(self):
        with self.store.bulk_insert() as bulk_insert:
            user1 = User(name="Aviv1")
            bulk_insert.store(user1)
            user_id1 = user1.Id

            user2 = User(name="Aviv2")
            bulk_insert.store(user2)
            user_id2 = user2.Id

            counter = bulk_insert.counters_for(user_id1)

            counter.increment("likes", 100)
            counter.increment("downloads", 500)

            bulk_insert.counters_for(user_id2).increment("votes", 1000)

        counters = self.store.operations.send(GetCountersOperation(user_id1, ["likes", "downloads"])).counters

        self.assertEqual(2, len(counters))

        counters.sort(key=lambda counter: counter.counter_name)

        self.assertEqual(500, counters[0].total_value)

        self.assertEqual(100, counters[1].total_value)

        val = self.store.operations.send(GetCountersOperation(user_id2, "votes"))
        self.assertEqual(1000, val.counters[0].total_value)

    def test_increment_many_counters(self):
        counter_count = 20000

        with self.store.bulk_insert() as bulk_insert:
            user1 = User(name="Aviv1")
            bulk_insert.store(user1)
            user1_id = user1.Id

            counter = bulk_insert.counters_for(user1_id)

            for i in range(1, counter_count + 1, 1):
                counter.increment(str(i), i)

        counters = self.store.operations.send(GetCountersOperation(user1_id)).counters

        self.assertEqual(counter_count, len(counters))

        for counter in counters:
            self.assertEqual(int(counter.counter_name), counter.total_value)
