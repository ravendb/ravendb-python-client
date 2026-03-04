from ravendb.documents.operations.attachments import StoreAttachmentParameters, RemoteAttachmentParameters
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestBulkInsertAttachments(TestBase):
    def setUp(self):
        super(TestBulkInsertAttachments, self).setUp()

    def test_store_async_null_id(self):
        def callback():
            with self.store.bulk_insert() as bulk_insert:
                bulk_insert.attachments_for(None)

        self.assertRaisesWithMessage(callback, ValueError, "Document id cannot be None or empty.")

    def test_store_attachment(self):
        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(name="John"), "users/1")
            attachments = bulk_insert.attachments_for("users/1")
            attachments.store("file.txt", bytes([1, 2, 3]), "text/plain")

        with self.store.open_session() as session:
            names = session.advanced.attachments.get_names(session.load("users/1", User))
            self.assertEqual(1, len(names))
            self.assertEqual("file.txt", names[0].name)
            self.assertEqual("text/plain", names[0].content_type)
            self.assertEqual(3, names[0].size)

    def test_store_attachment_with_parameters(self):
        attachment_bytes = bytes([10, 20, 30, 40, 50])

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(name="Jane"), "users/2")
            attachments = bulk_insert.attachments_for("users/2")
            params = StoreAttachmentParameters("photo.png", attachment_bytes, content_type="image/png")
            params.remote_parameters = RemoteAttachmentParameters("destination", datetime.datetime.now())
            attachments.store_with_parameters(params)

        with self.store.open_session() as session:
            names = session.advanced.attachments.get_names(session.load("users/2", User))
            self.assertEqual(1, len(names))
            self.assertEqual("photo.png", names[0].name)
            self.assertEqual("image/png", names[0].content_type)
            self.assertEqual(5, names[0].size)

    def test_store_multiple_attachments(self):
        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(name="Alice"), "users/3")
            attachments = bulk_insert.attachments_for("users/3")
            attachments.store("file1.txt", bytes([1, 2, 3]))
            attachments.store("file2.bin", bytes([4, 5, 6, 7]), "application/octet-stream")

        with self.store.open_session() as session:
            names = session.advanced.attachments.get_names(session.load("users/3", User))
            self.assertEqual(2, len(names))
            attachment_names = {a.name for a in names}
            self.assertIn("file1.txt", attachment_names)
            self.assertIn("file2.bin", attachment_names)
