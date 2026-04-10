"""
RDBC-1040: JavaScriptArray.remove_all(predicate_js) emits correct JS filter.

C# reference: JavaScriptArray<T>.RemoveAll(Func<T,bool>)
"""

import unittest

from ravendb.documents.session.misc import JavaScriptArray
from ravendb.tests.test_base import TestBase


class TestJavaScriptArrayRemoveAllUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_remove_all_generates_correct_js(self):
        arr = JavaScriptArray(0, "items")
        arr.remove_all("item > 5")
        script = arr.script
        self.assertIn("filter", script)
        self.assertIn("item > 5", script)

    def test_remove_all_uses_filter_assignment(self):
        arr = JavaScriptArray(0, "values")
        arr.remove_all("item === 'bad'")
        script = arr.script
        self.assertIn("this.values =", script)
        self.assertIn(".filter(", script)

    def test_remove_all_negates_predicate(self):
        arr = JavaScriptArray(0, "tags")
        arr.remove_all("item === 'x'")
        script = arr.script
        # The predicate should be negated with !()
        self.assertIn("!(", script)

    def test_remove_all_chaining_with_add(self):
        # C# uses expression trees so the compiler catches a broken add() at compile time.
        # In Python there is no equivalent static check, so both halves of the chain must
        # be asserted explicitly here.
        arr = JavaScriptArray(1, "tags")
        arr.add("new_tag").remove_all("item === 'old_tag'")
        script = arr.script
        self.assertIn("push", script)
        self.assertIn("filter", script)

    def test_remove_all_returns_self_for_chaining(self):
        arr = JavaScriptArray(0, "items")
        result = arr.remove_all("item > 0")
        self.assertIs(arr, result)


class TestJavaScriptArrayRemoveAllIntegration(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_remove_all_via_patch_removes_matching_items(self):
        from ravendb.documents.operations.patch import PatchOperation, PatchRequest

        class DocWithList:
            def __init__(self, items=None):
                self.items = items or []

        with self.store.open_session() as session:
            doc = DocWithList(items=[1, 2, 3, 4, 5, 6])
            session.store(doc, "docs/1")
            session.save_changes()

        patch_request = PatchRequest()
        arr = JavaScriptArray(0, "items")
        arr.remove_all("item > 3")
        patch_request.script = arr.script
        patch_request.values = {}

        self.store.operations.send(PatchOperation("docs/1", None, patch_request))

        with self.store.open_session() as session:
            loaded = session.load("docs/1", DocWithList)
            self.assertEqual([1, 2, 3], sorted(loaded.items))


if __name__ == "__main__":
    unittest.main()
