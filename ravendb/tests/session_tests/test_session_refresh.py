"""
Session refresh: advanced.refresh(entity) re-fetches from the server and updates
the entity object in-place; the refreshed entity stays tracked in the session.

C# reference: FastTests/Client/Store.cs
  Refresh_stored_document
"""

from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = "", age: int = 0):
        self.name = name
        self.age = age


class TestRavenDBRefreshStoredDocument(TestBase):
    def setUp(self):
        super().setUp()

    def test_refresh_updates_entity_in_place(self):
        """
        C# spec: after Advanced.Refresh(user), the SAME user object must
        reflect the server state.  No need to use the return value.

        must use the return value — but that returned entity is not tracked
        by the session (see test_refresh_returned_entity_is_tracked below)."""
        with self.store.open_session() as s:
            s.store(User(name="RVN", age=1), "users/1")
            s.save_changes()

        with self.store.open_session() as outer:
            user = outer.load("users/1", User)
            self.assertEqual(1, user.age, "precondition: age=1 after load")

            # External session updates age to 10 (mirrors C# inner session)
            with self.store.open_session() as inner:
                u2 = inner.load("users/1", User)
                u2.age = 10
                inner.save_changes()

            # C# spec: after Refresh(user), user.Age == 10
            outer.advanced.refresh(user)

            self.assertEqual(
                10,
                user.age,
                msg=f"refresh() did not update the entity in-place. user.age is still {user.age} (expected 10).",
            )

    def test_refresh_returned_entity_is_tracked(self):
        """
        refresh() returns the same (mutated-in-place) entity object.
        That returned value must be session-tracked so that metadata
        APIs such as get_change_vector_for() work on it."""
        with self.store.open_session() as s:
            s.store(User(name="RVN", age=1), "users/1")
            s.save_changes()

        with self.store.open_session() as outer:
            user = outer.load("users/1", User)

            with self.store.open_session() as inner:
                u2 = inner.load("users/1", User)
                u2.age = 10
                inner.save_changes()

            returned = outer.advanced.refresh(user)

            # The returned entity should be the refreshed copy.
            self.assertEqual(
                10,
                returned.age,
                msg="Return value of refresh() should have the updated age.",
            )

            # The returned entity must be session-tracked so metadata APIs work.
            cv = outer.advanced.get_change_vector_for(returned)
            self.assertIsNotNone(cv, "Change vector should be non-None for refreshed entity")

    def test_refresh_updates_change_vector_for_original_entity(self):
        """
        C# spec (Refresh_stored_document): after Refresh(user),
        Advanced.GetChangeVectorFor(user) must return the NEW change vector
        (not the one recorded at load time), and GetLastModifiedFor(user)
        must return the new timestamp."""
        with self.store.open_session() as s:
            s.store(User(name="RVN", age=1), "users/1")
            s.save_changes()

        with self.store.open_session() as outer:
            user = outer.load("users/1", User)
            cv_before = outer.advanced.get_change_vector_for(user)
            lm_before = outer.advanced.get_last_modified_for(user)

            with self.store.open_session() as inner:
                u2 = inner.load("users/1", User)
                u2.age = 10
                inner.save_changes()

            outer.advanced.refresh(user)

            cv_after = outer.advanced.get_change_vector_for(user)
            lm_after = outer.advanced.get_last_modified_for(user)

            self.assertNotEqual(
                cv_before,
                cv_after,
                msg="GetChangeVectorFor(user) must return a new change vector after Refresh()",
            )
            self.assertNotEqual(
                lm_before,
                lm_after,
                msg="GetLastModifiedFor(user) must return a new timestamp after Refresh()",
            )
