from pyravendb.tests.test_base import TestBase


class SimpleDoc:
    def __init__(self, Id=None, name=None):
        self.Id = Id
        self.name = name


class TestRavenDB15531(TestBase):
    def setUp(self):
        super(TestRavenDB15531, self).setUp()

    def test_should_work(self):
        with self.store.open_session() as session:
            doc = SimpleDoc("TestDoc", "State1")
            session.store(doc)
            session.save_changes()

            doc.name = "State2"
            changes1 = session.advanced.what_changed()
            changes = changes1["TestDoc"]
            self.assertIsNotNone(changes)
            self.assertEqual(1, len(changes))

            self.assertEqual(str(changes[0]["change"]), "field_changed")
            self.assertEqual(changes[0]["field_name"], "name")
            self.assertEqual(changes[0]["old_value"], "State1")
            self.assertEqual(changes[0]["new_value"], "State2")
            session.save_changes()

            doc.name = "State3"
            changes1 = session.advanced.what_changed()
            changes = changes1["TestDoc"]
            self.assertIsNotNone(changes)
            self.assertEqual(1, len(changes))

            self.assertEqual(str(changes[0]["change"]), "field_changed")
            self.assertEqual(changes[0]["field_name"], "name")
            self.assertEqual(changes[0]["old_value"], "State2")
            self.assertEqual(changes[0]["new_value"], "State3")

            doc = session.advanced.refresh(doc)

            doc.name = "State4"
            changes1 = session.advanced.what_changed()
            changes = changes1["TestDoc"]
            self.assertIsNotNone(changes)
            self.assertEqual(1, len(changes))

            self.assertEqual(str(changes[0]["change"]), "field_changed")
            self.assertEqual(changes[0]["field_name"], "name")
            self.assertEqual(changes[0]["old_value"], "State2")
            self.assertEqual(changes[0]["new_value"], "State4")
            session.save_changes()
