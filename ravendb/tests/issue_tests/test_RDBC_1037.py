"""
RDBC-1037: load_into_stream writes to BytesIO output; load_starting_with_into_stream
           takes an output parameter and writes to it.

C# reference: IAdvancedSessionOperations.LoadIntoStream() / LoadStartingWithIntoStream()
"""

import io
import json
import unittest

from ravendb.tests.test_base import TestBase


class TestLoadIntoStreamUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_load_into_stream_method_exists_on_advanced(self):
        from ravendb.documents.session.document_session import DocumentSession

        self.assertTrue(hasattr(DocumentSession._Advanced, "load_into_stream"))

    def test_load_starting_with_into_stream_method_exists(self):
        from ravendb.documents.session.document_session import DocumentSession

        self.assertTrue(hasattr(DocumentSession._Advanced, "load_starting_with_into_stream"))


class TestLoadIntoStream(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_load_into_stream_writes_to_bytesio(self):
        class Doc:
            def __init__(self, name: str = None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("alpha"), "docs/1")
            session.store(Doc("beta"), "docs/2")
            session.save_changes()

        output = io.BytesIO()
        with self.store.open_session() as session:
            session.advanced.load_into_stream(["docs/1", "docs/2"], output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        names = [r["name"] for r in data["Results"]]
        self.assertIn("alpha", names)
        self.assertIn("beta", names)

    def test_load_into_stream_single_document(self):
        class Doc:
            def __init__(self, name: str = None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("gamma"), "docs/3")
            session.save_changes()

        output = io.BytesIO()
        with self.store.open_session() as session:
            session.advanced.load_into_stream(["docs/3"], output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        self.assertEqual(1, len(data["Results"]))
        self.assertEqual("gamma", data["Results"][0]["name"])

    def test_load_starting_with_into_stream_writes_to_bytesio(self):
        class Doc:
            def __init__(self, name: str = None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("one"), "prefix/1")
            session.store(Doc("two"), "prefix/2")
            session.save_changes()

        output = io.BytesIO()
        with self.store.open_session() as session:
            session.advanced.load_starting_with_into_stream("prefix/", output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        self.assertEqual(2, len(data["Results"]))
        expected_names = {"one", "two"}
        for r in data["Results"]:
            self.assertIn(r["name"], expected_names)
            expected_names.discard(r["name"])
        self.assertEqual(0, len(expected_names))


if __name__ == "__main__":
    unittest.main()
