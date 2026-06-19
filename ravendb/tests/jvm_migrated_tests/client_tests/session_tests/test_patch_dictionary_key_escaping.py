import unittest
from typing import Optional

from ravendb.documents.session.misc import JavaScriptMap
from ravendb.tests.test_base import TestBase


class TestJavaScriptMapKeyEscaping(unittest.TestCase):
    def test_put_uses_bracket_notation_with_escaped_key(self):
        js_map = JavaScriptMap(0, "attrs")
        result = js_map.put("key.with.dots", 5)
        self.assertIs(result, js_map)  # fluent
        self.assertEqual('this.attrs["key.with.dots"] = args.val_0_0;', js_map.script)

    def test_put_escapes_quotes_in_key(self):
        js_map = JavaScriptMap(0, "attrs")
        js_map.put('he"llo', 1)
        self.assertEqual('this.attrs["he\\"llo"] = args.val_0_0;', js_map.script)

    def test_put_handles_numeric_key(self):
        js_map = JavaScriptMap(3, "attrs")
        js_map.put(7, "v")
        self.assertEqual('this.attrs["7"] = args.val_0_3;', js_map.script)

    def test_remove_uses_bracket_notation_and_is_fluent(self):
        js_map = JavaScriptMap(0, "attrs")
        result = js_map.remove("key.with.dots")
        self.assertIs(result, js_map)
        self.assertEqual('delete this.attrs["key.with.dots"];', js_map.script)

    def test_none_key_raises(self):
        with self.assertRaises(ValueError):
            JavaScriptMap(0, "attrs").put(None, 1)


class PatchDoc:
    def __init__(self, Id: Optional[str] = None, attributes: Optional[dict] = None):
        self.Id = Id
        self.attributes = attributes if attributes is not None else {}


class TestPatchObjectDictionaryKey(TestBase):
    def setUp(self):
        super().setUp()

    def test_patch_object_put_key_with_dots(self):
        with self.store.open_session() as session:
            session.store(PatchDoc(attributes={"existing": 1}), "docs/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.patch_object("docs/1", "attributes", lambda m: m.put("key.with.dots", "value"))
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("docs/1", PatchDoc)
            # The dotted key must be stored as a single literal key, not a nested path.
            self.assertEqual("value", doc.attributes["key.with.dots"])
            self.assertEqual(1, doc.attributes["existing"])


if __name__ == "__main__":
    unittest.main()
