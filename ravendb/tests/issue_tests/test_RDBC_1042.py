"""
RDBC-1042: Database-Missing header is URL-decoded; raises DatabaseDoesNotExistException.

C# reference: RavenDB-24435
"""

import unittest

from ravendb.exceptions.exceptions import DatabaseDoesNotExistException
from ravendb.tests.test_base import TestBase


class TestUnicodeDatabaseExceptionUnit(unittest.TestCase):
    """Unit tests that do not require a live server."""

    def test_database_does_not_exist_exception_importable(self):
        exc = DatabaseDoesNotExistException("my database")
        self.assertIn("my database", str(exc))

    def test_header_unquote_applied(self):
        # Simulate the server-set header value (percent-encoded) being decoded
        from urllib.parse import unquote

        encoded = "my%20db%20with%20spaces"
        exc = DatabaseDoesNotExistException(unquote(encoded))
        self.assertIn("my db with spaces", str(exc))
        self.assertNotIn("%20", str(exc))


class TestUnicodeDatabaseExceptionIntegration(TestBase):
    def test_missing_unicode_db_raises_correct_exception(self):
        from ravendb.documents.store.definition import DocumentStore

        base_urls = self.get_document_store().urls
        with DocumentStore(base_urls, "my db with spaces") as store:
            store.initialize()
            with self.assertRaises(DatabaseDoesNotExistException) as ctx:
                with store.open_session() as session:
                    session.load("docs/1")
            # Exception message should be decoded (no %20)
            msg = str(ctx.exception)
            self.assertNotIn("%20", msg)


if __name__ == "__main__":
    unittest.main()
