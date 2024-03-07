from ravendb.tests.test_base import TestBase


class TestRDBC815(TestBase):
    def setUp(self):
        super().setUp()

    def test_dict_into_collection(self):
        with self.store.open_session() as session:
            session.store({"name": "Gracjan"}, "Drivers/")
            session.save_changes()

        with self.store.open_session() as session:
            drivers = list(session.query_collection("Drivers"))
            self.assertEqual(1, len(drivers))
            self.assertEqual("Gracjan", drivers[0]["name"])
