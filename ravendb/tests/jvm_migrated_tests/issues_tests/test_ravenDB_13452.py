from typing import Dict

from ravendb.tests.test_base import TestBase

class Item:
    def __init__(self, values: Dict[str, str] = None):
        self.values = values

class TestRavenDB13452(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_modify_dictionary_with_patch_remove(self):
        with self.store.open_session() as session:
            item = Item()
            values = {}
            item.values = values
            values["Key1"] = "Value1"
            values["Key2"] = "Value2"
            values["Key3"] = "Value3"

            session.store(item, "items/1")
            session.save_changes()

        with self.store.open_session() as session:
            item = session.load("items/1", Item)
            session.advanced.patch_object(item, "values", lambda dict_: dict_.remove("Key2"))
            session.save_changes()

        with self.store.open_session() as session:
            item = session.load("items/1", dict)
            values = item.get("values")
            self.assertIsNotNone(values)
            self.assertEqual(2, len(values))

            self.assertEqual("Value1", values.get("Key1"))
            self.assertEqual("Value3", values.get("Key3"))


