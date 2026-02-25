"""C# ref: FastTests/Client/WhatChanged.cs — WhatChanged_should_be_idempotent_operation (RavenDB-9150)"""

from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = "", age: int = 0):
        self.name = name
        self.age = age


class TestRavenDBWhatChangedIdempotent(TestBase):
    def setUp(self):
        super().setUp()

    def test_what_changed_should_be_idempotent_operation(self):
        """No modifications — two calls return the same empty result."""
        with self.store.open_session() as session:
            session.store(User(name="Alice", age=30), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.load("users/1", User)

            result_1 = session.advanced.what_changed()
            result_2 = session.advanced.what_changed()

            self.assertEqual(result_1, result_2)

    def test_what_changed_should_be_idempotent_operation_with_changes(self):
        """With modifications — two calls return the same non-empty result."""
        with self.store.open_session() as session:
            session.store(User(name="user1"), "users/2")
            session.store(User(name="user2", age=1), "users/3")
            session.save_changes()

        with self.store.open_session() as session:
            user1 = session.load("users/2", User)
            user2 = session.load("users/3", User)

            user1.age = 10
            session.delete(user2)

            result_1 = session.advanced.what_changed()
            result_2 = session.advanced.what_changed()

            self.assertEqual(2, len(result_1))
            self.assertEqual(len(result_1), len(result_2))

    def test_what_changed_detects_metadata_modification(self):
        with self.store.open_session() as session:
            session.store(User(name="Bob"), "users/4")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/4", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@custom-tag"] = "v1"

            changes = session.advanced.what_changed()
            self.assertIn("users/4", changes)

    def test_what_changed_and_save_changes_agree_on_metadata_write(self):
        with self.store.open_session() as session:
            session.store(User(name="Carol"), "users/5")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/5", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@custom-tag"] = "v2"

            pending = session.advanced.what_changed()
            self.assertIn("users/5", pending)

            session.save_changes()

        with self.store.open_session() as session:
            reloaded_meta = session.advanced.get_metadata_for(session.load("users/5", User))
            self.assertEqual("v2", reloaded_meta.get("@custom-tag"))


class TestRavenDBWhatChangedMetadataOps(TestBase):
    def setUp(self):
        super().setUp()

    def test_has_changed_returns_true_for_metadata_only_modification(self):
        with self.store.open_session() as session:
            session.store(User(name="Alice"), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@custom"] = "v1"

            self.assertTrue(session.advanced.has_changed(user))

    def test_has_changed_returns_false_when_no_modification(self):
        with self.store.open_session() as session:
            session.store(User(name="Bob"), "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            session.load("users/2", User)
            user = session.load("users/2", User)

            self.assertFalse(session.advanced.has_changed(user))

    def test_has_changes_returns_true_for_metadata_only_modification(self):
        with self.store.open_session() as session:
            session.store(User(name="Carol"), "users/3")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/3", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@custom"] = "v1"

            self.assertTrue(session.advanced.has_changes())

    def test_has_changes_returns_false_when_no_modification(self):
        with self.store.open_session() as session:
            session.store(User(name="Dave"), "users/4")
            session.save_changes()

        with self.store.open_session() as session:
            session.load("users/4", User)
            self.assertFalse(session.advanced.has_changes())

    def test_what_changed_detects_metadata_key_deletion(self):
        with self.store.open_session() as session:
            session.store(User(name="Eve"), "users/5")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/5", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@custom"] = "to-be-deleted"
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/5", User)
            meta = session.advanced.get_metadata_for(user)
            self.assertEqual("to-be-deleted", meta.get("@custom"))
            del meta["@custom"]

            changes = session.advanced.what_changed()
            self.assertIn("users/5", changes)

    def test_metadata_deletion_is_detected_and_persisted(self):
        with self.store.open_session() as session:
            session.store(User(name="Frank"), "users/6")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/6", User)
            meta = session.advanced.get_metadata_for(user)
            meta["@tag"] = "remove-me"
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/6", User)
            meta = session.advanced.get_metadata_for(user)
            del meta["@tag"]

            self.assertIn("users/6", session.advanced.what_changed())
            session.save_changes()

        with self.store.open_session() as session:
            meta = session.advanced.get_metadata_for(session.load("users/6", User))
            self.assertNotIn("@tag", meta)


class Doc:
    def __init__(self, name: str = ""):
        self.name = name


class TestRavenDBWhatChangedMetadataMutations(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as session:
            d = Doc()
            session.store(d, "d/1")
            meta = session.advanced.get_metadata_for(d)
            meta["Test-A"] = ["a", "a", "a"]
            meta["Test-C"] = ["c", "c", "c"]
            session.save_changes()

    def test_metadata_array_value_change_detected(self):
        """meta["Test-A"] = ["b","a","c"] -> 2 ARRAY_VALUE_CHANGED entries."""
        with self.store.open_session() as session:
            d = session.load("d/1", Doc)
            meta = session.advanced.get_metadata_for(d)
            meta["Test-A"] = ["b", "a", "c"]

            changes = session.advanced.what_changed()

        self.assertIn("d/1", changes)
        self.assertEqual(2, len(changes["d/1"]))

    def test_metadata_key_removal_detected(self):
        """meta.Remove("Test-A") -> 1 REMOVED_FIELD entry."""
        with self.store.open_session() as session:
            d = session.load("d/1", Doc)
            meta = session.advanced.get_metadata_for(d)
            meta.pop("Test-A")

            changes = session.advanced.what_changed()

        self.assertIn("d/1", changes)
        change_types = [str(c["change"]) for c in changes["d/1"]]
        self.assertIn("removed_field", change_types)

    def test_metadata_remove_two_add_two_detected(self):
        """Remove Test-A, Test-C; add Test-B, Test-D -> 4 entries."""
        with self.store.open_session() as session:
            d = session.load("d/1", Doc)
            meta = session.advanced.get_metadata_for(d)
            meta.pop("Test-A")
            meta.pop("Test-C")
            meta["Test-B"] = ["b", "b", "b"]
            meta["Test-D"] = ["d", "d", "d"]

            changes = session.advanced.what_changed()

        self.assertIn("d/1", changes)
        self.assertEqual(4, len(changes["d/1"]))

    def test_metadata_remove_one_add_one_detected(self):
        """Remove Test-A; add Test-B -> 2 entries."""
        with self.store.open_session() as session:
            d = session.load("d/1", Doc)
            meta = session.advanced.get_metadata_for(d)
            meta.pop("Test-A")
            meta["Test-B"] = ["b", "b", "b"]

            changes = session.advanced.what_changed()

        self.assertIn("d/1", changes)
        self.assertEqual(2, len(changes["d/1"]))
