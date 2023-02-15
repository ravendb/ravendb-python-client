from ravendb.tests.test_base import TestBase


class TestBulkInsertAttachments(TestBase):
    def setUp(self):
        super(TestBulkInsertAttachments, self).setUp()

    def test_store_async_null_id(self):
        def callback():
            with self.store.bulk_insert() as bulk_insert:
                bulk_insert.attachments_for(None)

        self.assertRaisesWithMessage(callback, ValueError, "Document id cannot be None or empty.")
