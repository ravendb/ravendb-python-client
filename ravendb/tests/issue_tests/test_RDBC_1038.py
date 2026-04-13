"""
RDBC-1038: stream_into() implemented; DocumentQuery.to_stream() delegates to session.

C# reference: IDocumentSession.Advanced.StreamInto(), IDocumentQuery.ToStream()

StreamInto writes {"Results": [...]} JSON to the output stream, matching the C#
QueryIntoStream test which does JObject.Load(new JsonTextReader(new StreamReader(stream)))
and asserts json.GetValue("Results").Children().Count() == expected, then verifies
each result's field value.
"""

import io
import json
import unittest

from ravendb.tests.test_base import TestBase


class TestStreamIntoUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_stream_into_method_exists_on_advanced(self):
        from ravendb.documents.session.document_session import DocumentSession

        self.assertTrue(hasattr(DocumentSession._Advanced, "stream_into"))

    def test_to_stream_method_exists_on_document_query(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "to_stream"))

    def test_to_stream_method_exists_on_raw_document_query(self):
        from ravendb.documents.session.query import RawDocumentQuery

        self.assertTrue(hasattr(RawDocumentQuery, "to_stream"))

    def test_stream_into_rejects_non_query_argument(self):
        from ravendb.documents.session.document_session import DocumentSession

        advanced = object.__new__(DocumentSession._Advanced)
        advanced._session = object()
        with self.assertRaises(AttributeError):
            advanced.stream_into("not_a_query", io.BytesIO())


class TestStreamInto(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def _seed_documents(self):
        class Doc:
            def __init__(self, name: str = None, value: int = 0):
                self.name = name
                self.value = value

        with self.store.open_session() as session:
            for i in range(3):
                d = Doc(f"doc{i}", i)
                session.store(d, f"docs/{i}")
            session.save_changes()
        return Doc

    def test_stream_into_writes_results_json(self):
        Doc = self._seed_documents()

        output = io.BytesIO()
        with self.store.open_session() as session:
            query = session.query(object_type=Doc)
            session.advanced.stream_into(query, output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        self.assertEqual(3, len(data["Results"]))
        expected_names = {"doc0", "doc1", "doc2"}
        for r in data["Results"]:
            self.assertIn(r["name"], expected_names)
            expected_names.discard(r["name"])
        self.assertEqual(0, len(expected_names))

    def test_document_query_to_stream(self):
        Doc = self._seed_documents()

        output = io.BytesIO()
        with self.store.open_session() as session:
            query = session.query(object_type=Doc)
            query.to_stream(output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        self.assertEqual(3, len(data["Results"]))
        expected_names = {"doc0", "doc1", "doc2"}
        for r in data["Results"]:
            self.assertIn(r["name"], expected_names)
            expected_names.discard(r["name"])
        self.assertEqual(0, len(expected_names))

    def test_raw_document_query_to_stream(self):
        self._seed_documents()

        output = io.BytesIO()
        with self.store.open_session() as session:
            query = session.advanced.raw_query("FROM Docs")
            query.to_stream(output)

        output.seek(0)
        data = json.loads(output.read())
        self.assertIn("Results", data)
        self.assertEqual(3, len(data["Results"]))
        expected_names = {"doc0", "doc1", "doc2"}
        for r in data["Results"]:
            self.assertIn(r["name"], expected_names)
            expected_names.discard(r["name"])
        self.assertEqual(0, len(expected_names))


if __name__ == "__main__":
    unittest.main()
