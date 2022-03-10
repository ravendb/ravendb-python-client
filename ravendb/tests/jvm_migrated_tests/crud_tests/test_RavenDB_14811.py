import unittest

from ravendb.tests.test_base import TestBase, UserWithId


class UserProjectionIntId:
    def __init__(self, name=None, key="0"):
        self.Id = int(key)
        self.name = name


class UserProjectionStringId:
    def __init__(self, name=None, key=None):
        self.Id = str(key)
        self.name = name


class TestRavenDB14811(TestBase):
    def setUp(self):
        super(TestRavenDB14811, self).setUp()

    @unittest.skip("RDBC-466")
    def test_can_project_id_field_in_class(self):
        user = UserWithId("Grisha", 34)
        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            result = session.query(UserWithId).select(UserProjectionIntId(), "name").first()
            self.assertIsNotNone(result)
            self.assertEqual(0, result.Id)
            self.assertEqual(user.name, result.name)

        with self.store.open_session() as session:
            result = session.query(UserWithId).select(UserProjectionIntId(), ["Id"], ["name"]).first()
            self.assertIsNotNone(result)
            self.assertEqual(0, result.Id)
            self.assertEqual(user.Id, result.name)

    @unittest.skip("RDBC-466")
    def test_can_project_id_field(self):
        user = UserWithId("Grisha", 34)
        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            result = session.query(UserWithId).select(UserProjectionIntId(), "name").first()
            self.assertIsNotNone(result)
            self.assertEqual(user.name, result.name)

        with self.store.open_session() as session:  #                           fields       projections
            result = session.query(UserWithId).select(UserProjectionIntId(), ["age", "name"], ["Id", "name"]).first()
            self.assertIsNotNone(result)
            self.assertEqual(result.age, user.Id)
            self.assertEqual(result.name, user.name)

        with self.store.open_session() as session:  #                           *fields
            result = session.query(UserWithId).select(UserProjectionStringId(), "id", "name").first()
            self.assertIsNotNone(result)
            self.assertEqual(result.Id, user.Id)
            self.assertEqual(result.name, user.name)

        with self.store.open_session() as session:
            result = (
                session.query(UserWithId).select(UserProjectionStringId(), ["name", "name"], ["id", "name"]).first()
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.Id, user.name)
            self.assertEqual(result.name, user.name)
