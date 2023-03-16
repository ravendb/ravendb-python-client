from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.tests.test_base import TestBase, User


class TestRavenDB15134(TestBase):
    def setUp(self):
        super(TestRavenDB15134, self).setUp()

    def test_get_counters_operation_should_return_null_for_non_existing_counter(self):
        doc_id = "users/1"

        with self.store.open_session() as session:
            session.store(User(), doc_id)
            c = session.counters_for(doc_id)
            c.increment("likes")
            c.increment("dislikes", 2)
            session.save_changes()

        vals = self.store.operations.send(GetCountersOperation(doc_id, ["likes", "downloads", "dislikes"]))
        self.assertEqual(3, len(vals.counters))

        self.assertIn(None, vals.counters)
        self.assertEqual(1, len(list(filter(lambda x: x is not None and x.total_value == 1, vals.counters))))
        self.assertEqual(1, len(list(filter(lambda x: x is not None and x.total_value == 2, vals.counters))))

        vals = self.store.operations.send(GetCountersOperation(doc_id, ["likes", "downloads", "dislikes"], True))
        self.assertEqual(3, len(vals.counters))

        self.assertIn(None, vals.counters)
        self.assertTrue(len(list(filter(lambda x: x is not None and len(x.counter_values) == 1, vals.counters))) > 0)
