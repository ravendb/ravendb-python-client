"""C# ref: WhatChangedFor.cs, TrackEntity.cs, WhatChanged.cs"""

from ravendb.documents.session.misc import DocumentsChanges
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = "", age: int = 0):
        self.name = name
        self.age = age


class Obj:
    def __init__(self, id: str = None, a: str = None, b: str = None):
        self.Id = id
        self.A = a
        self.B = b


class Arr:
    def __init__(self, arr=None):
        self.arr = arr if arr is not None else []


class TestRavenDBSessionAdvancedApi(TestBase):
    def setUp(self):
        super().setUp()

    def test_what_changed_for_change_field(self):
        with self.store.open_session() as session:
            session.store(User(name="Alice"), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            user.age = 5

            result = session.advanced.what_changed_for(user)
            self.assertEqual(1, len(result))
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, result[0].change)

    def test_what_changed_for_new_field_before_save(self):
        """What_Changed_For_New_Field — first block: before SaveChanges."""
        with self.store.open_session() as session:
            user = User(name="Alice")
            session.store(user, "users/1")

            result = session.advanced.what_changed_for(user)
            self.assertEqual(1, len(result))
            self.assertEqual(DocumentsChanges.ChangeType.DOCUMENT_ADDED, result[0].change)

    def test_what_changed_for_new_field(self):
        """What_Changed_For_New_Field — second block: load as wider type."""
        with self.store.open_session() as session:
            session.store(User(name="Toli"), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            user.email = "toli@example.com"

            result = session.advanced.what_changed_for(user)
            self.assertEqual(1, len(result))
            self.assertEqual(DocumentsChanges.ChangeType.NEW_FIELD, result[0].change)

    def test_what_changed_for_removed_field(self):
        with self.store.open_session() as session:
            session.store(User(name="Toli", age=5), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            del user.age

            result = session.advanced.what_changed_for(user)
            self.assertEqual(1, len(result))
            self.assertEqual(DocumentsChanges.ChangeType.REMOVED_FIELD, result[0].change)

    def test_what_changed_for_delete_after_change_value(self):
        """RavenDB-13501: field changes discarded after delete in same session."""
        with self.store.open_session() as session:
            session.store(Obj(id="DEL", a="A", b="A"), "DEL")
            session.save_changes()

        with self.store.open_session() as session:
            o = session.load("DEL", Obj)
            o.A = "B"
            o.B = "C"
            session.delete(o)

            result = session.advanced.what_changed_for(o)
            self.assertEqual(1, len(result))
            self.assertEqual(DocumentsChanges.ChangeType.DOCUMENT_DELETED, result[0].change)

    def test_get_tracked_entities(self):
        with self.store.open_session() as session:
            user = User(name="Bob")
            session.store(user, "users/2")

            tracked = session.advanced.get_tracked_entities()
            self.assertIn("users/2", tracked)
            self.assertIs(user, tracked["users/2"]["entity"])
            self.assertFalse(tracked["users/2"]["is_deleted"])

    def test_get_tracked_entities_delete_by_id(self):
        with self.store.open_session() as session:
            session.store(User(name="Eve"), "users/3")
            session.save_changes()

        with self.store.open_session() as session:
            session.load("users/3", User)
            session.delete("users/3")

            tracked = session.advanced.get_tracked_entities()
            self.assertIn("users/3", tracked)
            self.assertTrue(tracked["users/3"]["is_deleted"])

    def test_get_tracked_entities_delete_by_entity(self):
        with self.store.open_session() as session:
            session.store(User(name="Frank"), "users/4")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/4", User)
            session.delete(user)

            tracked = session.advanced.get_tracked_entities()
            self.assertIn("users/4", tracked)
            self.assertTrue(tracked["users/4"]["is_deleted"])

    def test_what_changed_delete_after_change_value(self):
        """RavenDB-13501: what_changed() reports only DOCUMENT_DELETED after delete."""
        with self.store.open_session() as session:
            session.store(Obj(id="ABC", a="A", b="A"), "ABC")
            session.save_changes()

        with self.store.open_session() as session:
            o = session.load("ABC", Obj)
            o.A = "B"
            o.B = "C"
            session.delete(o)

            changes = session.advanced.what_changed()

        self.assertIn("ABC", changes)
        self.assertEqual(1, len(changes["ABC"]))
        self.assertEqual(DocumentsChanges.ChangeType.DOCUMENT_DELETED, changes["ABC"][0].change)


class TestWhatChangedForArrayChanges(TestBase):
    def setUp(self):
        super().setUp()

    def test_what_changed_for_array_value_changed(self):
        """["a",1,"b"] -> ["a",2,"c"] produces 2 ARRAY_VALUE_CHANGED entries."""
        with self.store.open_session() as session:
            session.store(Arr(arr=["a", 1, "b"]), "arr/1")
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("arr/1", Arr)
            arr.arr = ["a", 2, "c"]

            changes = session.advanced.what_changed_for(arr)

        self.assertEqual(2, len(changes))
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_CHANGED, changes[0].change)
        self.assertEqual(1, changes[0].field_old_value)
        self.assertEqual(2, changes[0].field_new_value)
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_CHANGED, changes[1].change)
        self.assertEqual("b", changes[1].field_old_value)
        self.assertEqual("c", changes[1].field_new_value)

    def test_what_changed_for_array_value_added(self):
        """["a",1,"b"] -> ["a",1,"b","c",2] produces 2 ARRAY_VALUE_ADDED entries."""
        with self.store.open_session() as session:
            session.store(Arr(arr=["a", 1, "b"]), "arr/1")
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("arr/1", Arr)
            arr.arr = ["a", 1, "b", "c", 2]

            changes = session.advanced.what_changed_for(arr)

        self.assertEqual(2, len(changes))
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_ADDED, changes[0].change)
        self.assertIsNone(changes[0].field_old_value)
        self.assertEqual("c", changes[0].field_new_value)
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_ADDED, changes[1].change)
        self.assertIsNone(changes[1].field_old_value)
        self.assertEqual(2, changes[1].field_new_value)

    def test_what_changed_for_array_value_removed(self):
        """["a",1,"b"] -> ["a"] produces 2 ARRAY_VALUE_REMOVED entries."""
        with self.store.open_session() as session:
            session.store(Arr(arr=["a", 1, "b"]), "arr/1")
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("arr/1", Arr)
            arr.arr = ["a"]

            changes = session.advanced.what_changed_for(arr)

        self.assertEqual(2, len(changes))
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_REMOVED, changes[0].change)
        self.assertEqual(1, changes[0].field_old_value)
        self.assertIsNone(changes[0].field_new_value)
        self.assertEqual(DocumentsChanges.ChangeType.ARRAY_VALUE_REMOVED, changes[1].change)
        self.assertEqual("b", changes[1].field_old_value)
        self.assertIsNone(changes[1].field_new_value)
