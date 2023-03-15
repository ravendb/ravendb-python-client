from typing import Optional, List

from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.tests.test_base import TestBase, User


class TestRavenDB15282(TestBase):
    def setUp(self):
        super(TestRavenDB15282, self).setUp()

    def test_counters_post_get_return_full_results(self):
        doc_id = "users/1"
        counter_names: List[Optional[str]] = [None] * 1000

        with self.store.open_session() as session:
            session.store(User(), doc_id)
            c = session.counters_for(doc_id)
            for i in range(1000):
                name = f"likes{i}"
                counter_names[i] = name
                c.increment(name)

            session.save_changes()

        vals = self.store.operations.send(GetCountersOperation(doc_id, counter_names, True))
        self.assertEqual(1000, len(vals.counters))

        for i in range(1000):
            self.assertEqual(1, len(vals.counters[i].counter_values))
            self.assertEqual(1, (next(vals.counters[i].counter_values.values().__iter__())))
