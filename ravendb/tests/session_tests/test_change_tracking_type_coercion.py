"""C# ref: FastTests/Client/WhatChanged.cs — RavenDB_8169"""

from ravendb.tests.test_base import TestBase


class Doc:
    def __init__(self, numbers=None):
        self.numbers = numbers if numbers is not None else []


class TestRavenDBWhatChangedTypeCoercion(TestBase):
    def setUp(self):
        super().setUp()

    def test_ravendb_8169(self):
        """int->str array change: [1,2,3] -> ["1","2","3"] produces 3 ARRAY_VALUE_CHANGED."""
        with self.store.open_session() as session:
            session.store(Doc(numbers=[1, 2, 3]), "docs/1")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("docs/1", Doc)
            doc.numbers = ["1", "2", "3"]

            changes = session.advanced.what_changed()

        self.assertIn("docs/1", changes)
        changed = [c for c in changes["docs/1"] if str(c["change"]) == "array_value_changed"]
        self.assertEqual(3, len(changed))

    def test_ravendb_8169_persisted(self):
        """int->str array change is persisted after save_changes()."""
        with self.store.open_session() as session:
            session.store(Doc(numbers=[1, 2, 3]), "docs/2")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("docs/2", Doc)
            doc.numbers = ["1", "2", "3"]
            session.save_changes()

        with self.store.open_session() as session:
            reloaded = session.load("docs/2", Doc)
            self.assertEqual(["1", "2", "3"], reloaded.numbers)


class ScalarDoc:
    def __init__(self, value=None, flag=None):
        self.value = value
        self.flag = flag


class TestRavenDBWhatChangedScalarTypeCoercion(TestBase):
    def setUp(self):
        super().setUp()

    def test_int_to_string_scalar_field_change_is_detected(self):
        """int 1 -> str "1" on a scalar field produces FIELD_CHANGED."""
        with self.store.open_session() as session:
            session.store(ScalarDoc(value=1), "scalar/1")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("scalar/1", ScalarDoc)
            doc.value = "1"

            changes = session.advanced.what_changed()

        self.assertIn("scalar/1", changes)
        field_changes = [c for c in changes["scalar/1"] if str(c["change"]) == "field_changed"]
        self.assertEqual(1, len(field_changes))

    def test_bool_to_int_scalar_field_change_is_detected(self):
        """bool True -> int 1 produces FIELD_CHANGED (Python: True == 1 is True)."""
        with self.store.open_session() as session:
            session.store(ScalarDoc(flag=True), "scalar/2")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("scalar/2", ScalarDoc)
            doc.flag = 1

            changes = session.advanced.what_changed()

        self.assertIn("scalar/2", changes)
        field_changes = [c for c in changes["scalar/2"] if str(c["change"]) == "field_changed"]
        self.assertEqual(1, len(field_changes))
