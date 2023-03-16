from ravendb.documents.operations.counters import GetCountersOperation
from ravendb.tests.test_base import TestBase, User


class TestRavenDB15313(TestBase):
    def setUp(self):
        super(TestRavenDB15313, self).setUp()

    def test_get_counters_operation_should_filter_duplicate_names__post_get(self):
        doc_id = "users/1"

        names = []
        dict = {}

        with self.store.open_session() as session:
            session.store(User(), doc_id)

            cf = session.counters_for(doc_id)

            for i in range(1024):
                if i % 4 == 0:
                    name = "abc"
                elif i % 10 == 0:
                    name = "xyz"
                else:
                    name = f"likes{i}"

                names.append(name)

                old_val = dict.get(name, None)
                dict[name] = (old_val + i) if old_val is not None else i

                cf.increment(name, i)

            session.save_changes()

        vals = self.store.operations.send(GetCountersOperation(doc_id, names))

        expected_count = len(dict)

        self.assertEqual(expected_count, len(vals.counters))

        hs = list(set(names))
        hs_names = list(filter(lambda x: True if (hs.remove(x) if x in hs else False) is None else False, names))  # :D
        expected_vals = list(map(lambda x: dict.get(x, None), hs_names))

        for i in range(len(hs_names)):
            name = hs_names[i]
            self.assertEqual(
                next(filter(lambda c: c.counter_name == name, vals.counters)).total_value, expected_vals[i]
            )

    def test_get_counters_operation_should_filter_duplicate_names(self):
        doc_id = "users/1"
        names = ["likes", "dislikes", "likes", "downloads", "likes", "downloads"]

        with self.store.open_session() as session:
            session.store(User(), doc_id)
            cf = session.counters_for(doc_id)

            for i in range(len(names)):
                cf.increment(names[i], i)

            session.save_changes()

        vals = self.store.operations.send(GetCountersOperation(doc_id, names))

        self.assertEqual(3, len(vals.counters))

        list.sort(vals.counters, key=lambda x: x.total_value)

        expected = 1  # dislikes
        self.assertEqual(expected, vals.counters[0].total_value)

        expected = 6  # likes
        self.assertEqual(expected, vals.counters[1].total_value)

        expected = 8
        self.assertEqual(expected, vals.counters[2].total_value)
