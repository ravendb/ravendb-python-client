from ravendb import DocumentStore
from ravendb.tests.test_base import TestBase


class DocName(dict):
    def __init__(self, x, y):
        super().__init__()
        self["x"] = x
        self["y"] = y
        self["z"] = 123


class TestRDBC664(TestBase):
    def setUp(self):
        super(TestRDBC664, self).setUp()

    def write_to_ravenDB(self, array1, array2, store: DocumentStore):
        with store.open_session() as session:
            for x, y in zip(array1, array2):
                x, y = int(x), int(y)
                input_to_store = DocName(x, y)
                session.store(input_to_store, "abc_%d" % x)

            session.save_changes()

    def query_ravenDB(self, query_string, store: DocumentStore):
        with store.open_session() as session:
            results = list(session.advanced.raw_query(query_string).wait_for_non_stale_results())
        return results

    def test_can_deserialize_documents_after_raw_query_with_no_object_type_specified(self):
        self.write_to_ravenDB([i for i in range(10)], [i * 3 for i in range(10)], self.store)
        results = self.query_ravenDB("from 'DocNames' where z==123", self.store)
        self.assertEqual(10, len(results))
