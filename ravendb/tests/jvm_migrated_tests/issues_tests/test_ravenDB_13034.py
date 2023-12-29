from ravendb.exceptions.raven_exceptions import ConcurrencyException
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB13034(TestBase):
    def setUp(self):
        super().setUp()

    def test_exploring_concurrency_behavior(self):
        with self.store.open_session() as s1:
            user = User(name="Nick", age=99)
            s1.store(user, "users/1-A")
            s1.save_changes()

        with self.store.open_session() as s2:
            s2.advanced.use_optimistic_concurrency = True
            u2 = s2.load("users/1-A", User)
            with self.store.open_session() as s3:
                u3 = s3.load("users/1-A", User)
                self.assertNotEqual(u2, u3)
                u3.age -= 1
                s3.save_changes()

            u2.age += 1

            u2_2 = s2.load("users/1-A", User)
            self.assertEqual(u2, u2_2)
            self.assertEqual(1, s2.advanced.number_of_requests)

            with self.assertRaises(RuntimeError):
                s2.save_changes()

            self.assertEqual(2, s2.advanced.number_of_requests)

            u2_3 = s2.load("users/1-A", User)
            self.assertEqual(u2_3, u2)
            self.assertEqual(2, s2.advanced.number_of_requests)

            with self.assertRaises(RuntimeError):
                s2.save_changes()

        with self.store.open_session() as s4:
            u4 = s4.load("users/1-A", User)
            self.assertEqual(98, u4.age)
