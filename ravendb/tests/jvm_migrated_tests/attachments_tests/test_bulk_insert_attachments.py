import datetime

from ravendb.documents.operations.attachments import (
    RemoteAttachmentFlags,
    RemoteAttachmentParameters,
    StoreAttachmentParameters,
)
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
            params.remote_parameters = RemoteAttachmentParameters(
                "destination", datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
            )
            attachments.store_with_parameters(params)

        with self.store.open_session() as session:
            names = session.advanced.attachments.get_names(session.load("users/2", User))
            self.assertEqual(1, len(names))
            self.assertEqual("photo.png", names[0].name)
            self.assertEqual("image/png", names[0].content_type)
            self.assertEqual(5, names[0].size)

    def test_store_remote_attachment_persists_remote_parameters(self):
        """remote_parameters (identifier, at, flags=None) survive the bulk insert round-trip."""
        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(name="Bob"), "users/remote-1")
            attachments = bulk_insert.attachments_for("users/remote-1")
            params = StoreAttachmentParameters("report.pdf", bytes([1, 2, 3, 4]), content_type="application/pdf")
            params.remote_parameters = RemoteAttachmentParameters("dest-1", at)
            attachments.store_with_parameters(params)

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get("users/remote-1", "report.pdf")
            self.assertIsNotNone(attachment)
            self.assertEqual("report.pdf", attachment.details.name)
            self.assertEqual("application/pdf", attachment.details.content_type)
            self.assertIsNotNone(attachment.details.remote_parameters)
            self.assertEqual("dest-1", attachment.details.remote_parameters.identifier)
            self.assertEqual(RemoteAttachmentFlags.NONE, attachment.details.remote_parameters.flags)
            self.assertEqual(
                at.replace(microsecond=0, tzinfo=None), attachment.details.remote_parameters.at.replace(microsecond=0)
            )

    def test_store_multiple_remote_attachments_on_same_document(self):
        """Two remote attachments on the same document, each with a different identifier."""
        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(name="Carol"), "users/remote-2")
            attachments = bulk_insert.attachments_for("users/remote-2")

            p1 = StoreAttachmentParameters("photo.jpg", bytes([10, 20, 30]), content_type="image/jpeg")
            p1.remote_parameters = RemoteAttachmentParameters("dest-1", at)
            attachments.store_with_parameters(p1)

            p2 = StoreAttachmentParameters("thumb.jpg", bytes([1, 2]), content_type="image/jpeg")
            p2.remote_parameters = RemoteAttachmentParameters("dest-2", at)
            attachments.store_with_parameters(p2)

        with self.store.open_session() as session:
            user = session.load("users/remote-2", User)
            names = session.advanced.attachments.get_names(user)
            self.assertEqual(2, len(names))
            name_set = {a.name for a in names}
            self.assertIn("photo.jpg", name_set)
            self.assertIn("thumb.jpg", name_set)

            a1 = session.advanced.attachments.get("users/remote-2", "photo.jpg")
            self.assertEqual("dest-1", a1.details.remote_parameters.identifier)

            a2 = session.advanced.attachments.get("users/remote-2", "thumb.jpg")
            self.assertEqual("dest-2", a2.details.remote_parameters.identifier)

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
