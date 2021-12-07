from pyravendb.custom_exceptions.exceptions import NonUniqueObjectException, InvalidOperationException
from pyravendb.tests.test_base import UserWithId, TestBase


class TestTrackEntity(TestBase):
    def setUp(self):
        super(TestTrackEntity, self).setUp()

    def test_storing_document_with_the_same_id_in_the_same_session_should_throw(self):
        with self.store.open_session() as session:
            user = UserWithId("User1", None, identifier="users/1")
            session.store(user)
            session.save_changes()

            new_user = UserWithId("User2", None, identifier="users/1")
            ex_message = "Attempted to associate a different object with id 'users/1'."
            self.assertRaisesWithMessage(session.store, NonUniqueObjectException, ex_message, new_user)

    def test_deleting_entity_that_is_not_tracked_should_throw(self):
        with self.store.open_session() as session:
            user = UserWithId(None, None)
            ex_message = f"{user} is not associated with the session, cannot delete unknown entity instance."
            self.assertRaisesWithMessage(session.delete, InvalidOperationException, ex_message, user)

    def test_loading_deleted_document_should_return_null(self):
        with self.store.open_session() as session:
            user1 = UserWithId("John", None, "users/1")
            user2 = UserWithId("Jonathan", None, "users/2")
            session.store(user1)
            session.store(user2)
            session.save_changes()

        with self.store.open_session() as session:
            session.delete("users/1")
            session.delete("users/2")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertIsNone(session.load("users/1", UserWithId))
            self.assertIsNone(session.load("users/2", UserWithId))
