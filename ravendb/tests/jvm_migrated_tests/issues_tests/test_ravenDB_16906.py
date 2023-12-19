from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB16906(TestBase):
    def setUp(self):
        super().setUp()

    def test_time_series_for_should_throw_better_error_on_null_entity(self):
        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertRaisesWithMessage(
                session.time_series_for_entity, ValueError, "Entity cannot be None", user, "heartRate"
            )
