from ravendb.tests.test_base import TestBase


class TestIssue172(TestBase):
    def setUp(self):
        super().setUp()

    def test_get_metadata(self):
        with self.store.open_session() as ravendb_session:
            ravendb_session.store({"a": 1}, "a+b")
            ravendb_session.store({"b": 2}, "a-b")
            ravendb_session.store({"c": 3}, "%:=&?~#+!$,;'*[]")
            ravendb_session.save_changes()

        with self.store.open_session() as session:
            a = session.load("a+b")
            b = session.load("a-b")
            c = session.load("%:=&?~#+!$,;'*[]")
            self.assertIsNotNone(a)
            self.assertIsNotNone(b)
            self.assertIsNotNone(c)
