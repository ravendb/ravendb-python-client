from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRDBC387(TestBase):
    def setUp(self):
        super(TestRDBC387, self).setUp()

    def test_should_store_single_id(self):
        with self.store.open_session() as session:
            u = User(name="John")
            session.store(u)
            session.save_changes()
            user_id_via_session_id = u.Id

        with self.store.bulk_insert() as bulk_insert:
            u = User(name="Marcin")
            bulk_insert.store(u)
            user_id_via_bulk_insert = u.Id

        with self.store.open_session() as session:
            session_user = session.load(user_id_via_session_id, dict)
            bulk_insert_user = session.load(user_id_via_bulk_insert, dict)

            self.assertIsNone(session_user.get("id", None))
            self.assertIsNone(bulk_insert_user.get("id", None))

            self.assertIsNotNone(session_user.get("Id", None))
            self.assertIsNone(bulk_insert_user.get("Id", None))
