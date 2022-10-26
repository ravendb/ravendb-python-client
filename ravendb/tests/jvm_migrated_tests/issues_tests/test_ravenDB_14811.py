from ravendb import QueryData
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, Id: str = None, name: str = None, age: int = None):
        self.Id = Id
        self.name = name
        self.age = age


class UserProjectionId:
    def __init__(self, Id: int = None, name: str = None):
        self.Id = Id
        self.name = name


class TestRavenDB14811(TestBase):
    def setUp(self):
        super(TestRavenDB14811, self).setUp()

    def test_can_project_id_field(self):
        user = User()
        user.name = "Grisha"
        user.age = 34
        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            result = session.query(object_type=User).select_fields(UserProjectionId, "name").first()
            self.assertIsNotNone(result)
            self.assertEqual(user.name, result.name)

        with self.store.open_session() as session:
            result = (
                session.query(object_type=User)
                .select_fields_query_data(UserProjectionId, QueryData(["age", "name"], ["Id", "name"]))
                .first()
            )

            self.assertIsNotNone(result)
            self.assertEqual(result.Id, user.age)
            self.assertEqual(user.name, result.name)

    def test_can_project_id_field_in_class(self):
        user = User()
        user.name = "Grisha"
        user.age = 34
        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            result = session.query(object_type=User).select_fields(UserProjectionId, "name").first()
            self.assertIsNotNone(result)
            self.assertIsNone(result.Id)
            self.assertEqual(user.name, result.name)

        with self.store.open_session() as session:
            result = (
                session.query(object_type=User)
                .select_fields_query_data(UserProjectionId, QueryData(["Id"], ["name"]))
                .first()
            )

            self.assertIsNotNone(result)
            self.assertIsNone(result.Id)
            self.assertEqual(user.Id, result.name)
