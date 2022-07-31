from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class ConditionalLoadTest(TestBase):
    def setUp(self):
        super(ConditionalLoadTest, self).setUp()

    def test_conditional_load_can_get_document_by_id(self):
        with self.store.open_session() as session:
            user = User(name="RavenDB")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as new_session:
            user = new_session.load("users/1", User)
            cv = new_session.get_change_vector_for(user)
            self.assertIsNotNone(user)
            self.assertEqual("RavenDB", user.name)
            user.name = "RavenDB 5.1"
            new_session.save_changes()

        with self.store.open_session() as newest_session:
            user = newest_session.advanced.conditional_load("users/1", cv, User)
            self.assertEqual("RavenDB 5.1", user.entity.name)

    def test_conditional_load_can_get_not_modified_document_by_id_should_return_null(self):
        with self.store.open_session() as session:
            user = User(name="RavenDB")
            session.store(user, "users/1")
            session.save_changes()

        cv = None

        with self.store.open_session() as new_session:
            user = new_session.load("users/1", User)
            self.assertIsNotNone(user)
            self.assertEqual("RavenDB", user.name)
            user.name = "RavenDB 5.1"
            new_session.save_changes()
            cv = new_session.get_change_vector_for(user)

        with self.store.open_session() as newest_session:
            user = newest_session.advanced.conditional_load("users/1", cv, User)
            self.assertIsNone(user.entity)
            self.assertEqual(cv, user.change_vector)

    def test_conditional_load_non_exists_document_should_return_null(self):
        with self.store.open_session() as session:
            user = User(name="RavenDB")
            session.store(user, "users/1")
            session.save_changes()

        cv = None

        with self.store.open_session() as new_session:
            user = new_session.load("users/1", User)
            self.assertIsNotNone(user)
            self.assertEqual("RavenDB", user.name)
            user.name = "RavenDB 5.1"
            new_session.save_changes()
            cv = new_session.get_change_vector_for(user)

        with self.store.open_session() as newest_session:
            with self.assertRaises(ValueError):
                newest_session.advanced.conditional_load("users/2", None, User)

            result = newest_session.advanced.conditional_load("users/2", cv, User)
            self.assertIsNone(result.entity)
            self.assertIsNone(result.change_vector)
            self.assertTrue(newest_session.is_loaded("users/2"))

            expected = newest_session.advanced.number_of_requests
            newest_session.load("users/2", User)

            self.assertEqual(expected, newest_session.advanced.number_of_requests)
