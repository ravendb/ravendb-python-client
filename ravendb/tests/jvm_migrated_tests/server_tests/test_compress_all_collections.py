from ravendb import GetDatabaseRecordOperation, DocumentsCompressionConfiguration
from ravendb.serverwide.operations.documents_compression import UpdateDocumentsCompressionConfigurationOperation
from ravendb.tests.test_base import TestBase


class TestCompressAllCollections(TestBase):
    def setUp(self):
        super(TestCompressAllCollections, self).setUp()

    def test_compress_all_collections_after_docs_change(self):
        # we are running in memory - just check if command will be sent to server
        self.store.maintenance.send(
            UpdateDocumentsCompressionConfigurationOperation(DocumentsCompressionConfiguration(False, True))
        )

        record = self.store.maintenance.server.send(GetDatabaseRecordOperation(self.store.database))

        documents_compression = record.documents_compression

        self.assertTrue(documents_compression.compress_all_collections)
        self.assertFalse(documents_compression.compress_revisions)
