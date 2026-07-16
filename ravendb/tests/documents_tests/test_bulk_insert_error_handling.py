"""
BulkInsert: server errors surface as BulkInsertAbortedException with the
server's error detail; metadata round-trips correctly.

C# reference: FastTests/Client/BulkInsert/BulkInserts.cs
  CanModifyMetadataWithBulkInsert
"""

import datetime

from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.primitives.constants import Documents
from ravendb.exceptions.documents.bulkinsert import BulkInsertAbortedException
from ravendb.tests.test_base import TestBase


class FooBar:
    def __init__(self, name: str = ""):
        self.name = name


class TestRavenDBBulkInsert(TestBase):
    def setUp(self):
        super().setUp()

    def test_bulk_insert_server_error_raises_aborted_exception(self):
        """
        When the server rejects a bulk insert operation,
        _get_exception_from_operation returns BulkInsertAbortedException
        carrying the server's error text.

        C# ref: BulkInserts.CanModifyMetadataWithBulkInsert (error path)
        """
        # A date-only string triggers a server-side error (@expires requires a full datetime).
        date_only_expiry = (datetime.date.today() + datetime.timedelta(days=365)).isoformat()
        meta = MetadataAsDictionary()
        meta[Documents.Metadata.EXPIRES] = date_only_expiry

        with self.assertRaises(BulkInsertAbortedException) as cm:
            with self.store.bulk_insert() as bi:
                bi.store(FooBar(name="Jon Snow"), metadata=meta)

        error_message = str(cm.exception)
        self.assertNotIn(
            "Failed to execute bulk insert",
            error_message,
            "exception message should contain server error detail, not just the generic fallback",
        )

    def test_bulk_insert_with_full_datetime_expiry_works(self):
        """
        C# spec: BulkInserts.CanModifyMetadataWithBulkInsert — stores a document
        with @expires set to a full datetime string; the server accepts it and
        persists the expiry in metadata.
        """
        expiry = (datetime.datetime.utcnow() + datetime.timedelta(days=365)).isoformat()
        meta = MetadataAsDictionary()
        meta[Documents.Metadata.EXPIRES] = expiry

        with self.store.bulk_insert() as bi:
            bi.store(FooBar(name="Jon Snow"), metadata=meta)

        with self.store.open_session() as session:
            entity = session.load("FooBars/1-A", FooBar)
            self.assertIsNotNone(entity, "Document should have been stored")
            meta_out = session.advanced.get_metadata_for(entity)
            stored_expiry = meta_out.get(Documents.Metadata.EXPIRES)
            self.assertIsNotNone(stored_expiry, "@expires should be persisted in metadata")
            self.assertEqual(expiry, stored_expiry, "@expires value should round-trip exactly")
