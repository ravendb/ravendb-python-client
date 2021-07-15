from pyravendb.tests.test_base import TestBase, User


class TestCrud(TestBase):
    def setUp(self):
        super(TestCrud, self).setUp()

    def test_update_property_from_null_to_object(self):
        poc = Poc("jacek", None)

        with self.store.open_session() as session:
            session.store(poc, "pocs/1")
            session.save_changes()

        with self.store.open_session() as session:
            poc = session.load("pocs/1", Poc)
            self.assertIsNone(poc.user)

            poc.user = User(None, None)
            session.save_changes()

        with self.store.open_session() as session:
            poc = session.load("pocs/1", Poc)
            self.assertIsNotNone(poc)


class Poc:
    def __init__(self, name, user):
        self.name = name
        self.user = user
