import unittest

from pyravendb.tests.test_base import TestBase, User
from dataclasses import dataclass


@dataclass()
class TestData:
    data: str
    Id: str


class BasicNamedUser:
    def __init__(self, basic_name=None, age=None, Id=None):
        self.name = basic_name
        self.age = age
        self.Id = Id


class BasicName:
    def __init__(self, name):
        self.name = name


class BasicAge:
    def __init__(self, age):
        self.age = age


class Arr:
    def __init__(self, arr: list):
        self.arr = arr


class Int:
    def __init__(self, number=0):
        self.number = number


class Double:
    def __init__(self, number=0.0):
        self.number = number


class TestWhatChanged(TestBase):
    def setUp(self):
        super(TestWhatChanged, self).setUp()

    def test_has_changes(self):
        with self.store.open_session() as session:
            user1 = User("user1", None)
            user2 = User("user2", 1)

            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.has_changes())

            users = session.load(["users/1", "users/2"], User)
            user1 = users["users/1"]
            user2 = users["users/2"]
            self.assertFalse(session.has_changed(user1))
            self.assertFalse(session.has_changed(user2))

            user1.name = "newName"

            self.assertTrue(session.has_changed(user1))
            self.assertFalse(session.has_changed(user2))
            self.assertTrue(session.has_changes())

    def test_what_changed_new_field(self):
        with self.store.open_session() as session:
            basic_name = BasicName("Toli")
            session.store(basic_name, "users/1")
            changes = session.advanced.what_changed()
            self.assertEqual(len(changes), 1)
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", BasicName)
            user.age = 5
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual("new_field", str(changes["users/1"][0]["change"]))
            session.save_changes()

    def test_what_changed_remove_field(self):
        with self.store.open_session() as session:
            user = User("Toli", 5)
            session.store(user, "users/1")
            self.assertEqual(1, len(session.advanced.what_changed()))
            session.save_changes()

        with self.store.open_session() as session:
            u = session.load("users/1", User)
            del u.name
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual("removed_field", str(changes["users/1"][0]["change"]))
            session.save_changes()

    # its not the same yet its valid
    def test_what_changed_change_field(self):
        with self.store.open_session() as session:
            user = BasicAge(10)
            session.store(user, "users/1")
            self.assertEqual(1, len(session.advanced.what_changed()))
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", BasicAge)
            user.age = 6
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual("field_changed", str(changes["users/1"][0]["change"]))
            session.save_changes()

    def test_what_changed_array_value_changed(self):
        with self.store.open_session() as session:
            session.store(Arr(["a", 1, "b"]), "users/1")
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual(1, len(changes["users/1"]))
            self.assertEqual("document_added", str(changes["users/1"][0]["change"]))
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("users/1", Arr)
            arr.arr = ["a", 2, "c"]
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))

            self.assertEqual(2, len(changes["users/1"]))

            self.assertEqual("array_value_changed", str(changes["users/1"][0]["change"]))
            self.assertEqual("1", str(changes["users/1"][0]["old_value"]))
            self.assertEqual(2, changes["users/1"][0]["new_value"])

            self.assertEqual("array_value_changed", str(changes["users/1"][1]["change"]))
            self.assertEqual("b", str(changes["users/1"][1]["old_value"]))
            self.assertEqual("c", str(changes["users/1"][1]["new_value"]))

    def test_what_changed_array_value_added(self):
        with self.store.open_session() as session:
            session.store(Arr(["a", 1, "b"]), "arr/1")
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("arr/1", Arr)
            arr.arr = ["a", 1, "b", "c", 2]
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual(2, len(changes["arr/1"]))

            self.assertEqual("array_value_added", str(changes["arr/1"][0]["change"]))
            self.assertEqual("c", str(changes["arr/1"][0]["new_value"]))
            self.assertEqual(None, changes["arr/1"][0]["old_value"])

            self.assertEqual("array_value_added", str(changes["arr/1"][1]["change"]))
            self.assertEqual(2, changes["arr/1"][1]["new_value"])
            self.assertIsNone(changes["arr/1"][1]["old_value"])

    def test_what_changed_array_value_removed(self):
        with self.store.open_session() as session:
            session.store(Arr(["a", 1, "b"]), "arr/1")
            session.save_changes()

        with self.store.open_session() as session:
            arr = session.load("arr/1", Arr)
            arr.arr = ["a"]
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual(2, len(changes["arr/1"]))

            self.assertEqual("array_value_removed", str(changes["arr/1"][1]["change"]))
            self.assertEqual("b", str(changes["arr/1"][1]["old_value"]))
            self.assertIsNone(changes["arr/1"][1]["new_value"])

    def test_what_changed_should_be_idempotent_operation(self):
        # RavenDB-9150
        with self.store.open_session() as session:
            session.store(User("user1", 1), "users/1")
            session.store(User("user2", 2), "users/2")
            session.store(User("user3", 3), "users/3")
            changes = session.advanced.what_changed()
            self.assertEqual(3, len(changes))
            session.save_changes()

            user1 = session.load("users/1", User)
            user2 = session.load("users/2", User)
            user1.age = 10
            session.delete(user2)
            self.assertEqual(2, len(session.advanced.what_changed()))
            self.assertEqual(2, len(session.advanced.what_changed()))

    @unittest.skip("Waiting for proper projections implementation")
    def test_ravenDB_8169(self):
        # Test that when old and new values are of different type
        # but have the same value, we consider them unchanged
        with self.store.open_session() as session:
            session.store(Int(1), "num/1")
            session.store(Double(2.0), "num/2")
            session.save_changes()

        with self.store.open_session() as session:
            session.load("num/1", Double)
            changes = session.advanced.what_changed()
            self.assertEqual(0, len(changes))

        with self.store.open_session() as session:
            session.load("num/2", Int)
            changes = session.advanced.what_changed()
            self.assertEqual(0, len(changes))

    def test_what_changed_unhashables(self):
        data = TestData(data="classified", Id="")
        with self.store.open_session() as session:
            self.assertEqual("", data.Id)
            session.store(data)
            self.assertEqual("TestDatas/1-A", data.Id)

            changes = session.advanced.what_changed()

            self.assertEqual(1, len(changes))
            self.assertEqual(1, len(changes["TestDatas/1-A"]))
            self.assertEqual("document_added", changes["TestDatas/1-A"][0]["change"])
            session.save_changes()

            data.data = "covert"
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes["TestDatas/1-A"]))

            doc_changes = changes["TestDatas/1-A"][0]
            self.assertEqual("field_changed", doc_changes["change"])
            self.assertEqual("data", doc_changes["field_name"])
            self.assertEqual("classified", doc_changes["old_value"])
            self.assertEqual("covert", doc_changes["new_value"])
            session.save_changes()

        with self.store.open_session() as session:
            data = session.load("TestDatas/1-A", TestData)
            session.delete(data)
            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes["TestDatas/1-A"]))
            self.assertEqual("document_deleted", changes["TestDatas/1-A"][0]["change"])
