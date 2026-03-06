"""
Tests for storing attachments with RemoteAttachmentParameters via the document session.

Migrated from:
  test/SlowTests/Server/Documents/Attachments/DocumentSessionRemoteAttachmentsAsyncTests.cs

Scope: client-side session API only.
Tests that require the server-side RemoteAttachmentsSender (ProcessRemoteAttachments) to
actually push bytes to S3/Azure — and therefore need real cloud credentials — are omitted
here. There are separate mock tests for that purpose.

What IS tested:
  - store_with_parameters stores the attachment and persists RemoteAttachmentParameters
    (identifier, at, flags=None) as returned by session.advanced.attachments.get()
  - exists() returns True after storing with remote params
  - delete() removes the attachment
  - get() by entity object (not just document id)
  - get_names() reflects the attachment
  - overwriting an attachment with new remote params replaces the old params
  - storing with remote_parameters=None produces a plain attachment (no remote_parameters)
  - storing to multiple destinations (two different identifiers) works independently
"""

import datetime
import unittest

from ravendb.documents.operations.attachments import (
    RemoteAttachmentFlags,
    RemoteAttachmentParameters,
    StoreAttachmentParameters,
)
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str):
        self.name = name


_IDENTIFIER = "Conf-identifier-s3-1"
_IDENTIFIER_2 = "Conf-identifier-s3-2"
_AT = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)


class TestDocumentSessionRemoteAttachments(TestBase):
    def setUp(self):
        super().setUp()

    # ── helpers ──────────────────────────────────────────────────────────────

    def _store_doc(self, doc_id: str) -> None:
        with self.store.open_session() as session:
            session.store(User("Alice"), doc_id)
            session.save_changes()

    def _store_attachment(
        self, doc_id: str, name: str, data: bytes, identifier: str, at=None, content_type="image/png"
    ):
        params = StoreAttachmentParameters(name, data, content_type=content_type)
        params.remote_parameters = RemoteAttachmentParameters(identifier, at or _AT)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters(doc_id, params)
            session.save_changes()

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_can_store_and_get_remote_attachment_metadata(self):
        """store_with_parameters persists identifier, at, and flags=None."""
        doc_id = "orders/1"
        self._store_doc(doc_id)
        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER, at)

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get(doc_id, "test.png")
            self.assertIsNotNone(attachment)
            self.assertEqual("test.png", attachment.details.name)
            self.assertEqual("image/png", attachment.details.content_type)
            self.assertIsNotNone(attachment.details.remote_parameters)
            self.assertEqual(RemoteAttachmentFlags.NONE, attachment.details.remote_parameters.flags)
            self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)
            # compare truncated to seconds to avoid sub-second drift
            self.assertEqual(
                at.replace(microsecond=0, tzinfo=None), attachment.details.remote_parameters.at.replace(microsecond=0)
            )

    def test_can_check_if_remote_attachment_exists(self):
        doc_id = "orders/2"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        with self.store.open_session() as session:
            self.assertTrue(session.advanced.attachments.exists(doc_id, "test.png"))

    def test_can_get_remote_attachment_by_entity(self):
        doc_id = "orders/3"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        with self.store.open_session() as session:
            order = session.load(doc_id, User)
            attachment = session.advanced.attachments.get(order, "test.png")
            self.assertIsNotNone(attachment)
            self.assertEqual("test.png", attachment.details.name)
            self.assertIsNotNone(attachment.details.remote_parameters)
            self.assertEqual(RemoteAttachmentFlags.NONE, attachment.details.remote_parameters.flags)

    def test_can_delete_remote_attachment(self):
        doc_id = "orders/4"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        with self.store.open_session() as session:
            session.advanced.attachments.delete(doc_id, "test.png")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists(doc_id, "test.png"))

    def test_can_delete_remote_attachment_by_entity(self):
        doc_id = "orders/5"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        with self.store.open_session() as session:
            order = session.load(doc_id, User)
            session.advanced.attachments.delete(order, "test.png")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists(doc_id, "test.png"))

    def test_get_names_reflects_remote_attachment(self):
        doc_id = "orders/6"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        with self.store.open_session() as session:
            order = session.load(doc_id, User)
            names = session.advanced.attachments.get_names(order)
            self.assertEqual(1, len(names))
            self.assertEqual("test.png", names[0].name)
            self.assertEqual("image/png", names[0].content_type)

    def test_can_overwrite_remote_attachment_with_new_params(self):
        """Overwriting with new identifier/at replaces the old remote params."""
        doc_id = "orders/7"
        self._store_doc(doc_id)
        at1 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER, at1)

        at2 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=15)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER_2, at2)

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get(doc_id, "test.png")
            self.assertIsNotNone(attachment)
            self.assertEqual(_IDENTIFIER_2, attachment.details.remote_parameters.identifier)
            self.assertEqual(
                at2.replace(microsecond=0, tzinfo=None), attachment.details.remote_parameters.at.replace(microsecond=0)
            )

    def test_can_store_plain_attachment_after_remote(self):
        """Overwriting a remote attachment with remote_parameters=None clears remote params."""
        doc_id = "orders/8"
        self._store_doc(doc_id)
        self._store_attachment(doc_id, "test.png", bytes([1, 2, 3]), _IDENTIFIER)

        # overwrite with plain (no remote params)
        with self.store.open_session() as session:
            session.advanced.attachments.store(doc_id, "test.png", bytes([4, 5, 6]), content_type="image/png")
            session.save_changes()

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get(doc_id, "test.png")
            self.assertIsNotNone(attachment)
            self.assertIsNone(attachment.details.remote_parameters)

    def test_can_store_to_multiple_destinations(self):
        """Two documents each get an attachment pointing to a different identifier."""
        id1, id2 = "orders/9", "orders/10"
        self._store_doc(id1)
        self._store_doc(id2)
        at1 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        at2 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        self._store_attachment(id1, "test.png", bytes([1, 2, 3]), _IDENTIFIER, at1)
        self._store_attachment(id2, "test.png", bytes([3, 2, 1]), _IDENTIFIER_2, at2)

        with self.store.open_session() as session:
            a1 = session.advanced.attachments.get(id1, "test.png")
            self.assertEqual(_IDENTIFIER, a1.details.remote_parameters.identifier)
            self.assertEqual(
                at1.replace(microsecond=0, tzinfo=None), a1.details.remote_parameters.at.replace(microsecond=0)
            )

            a2 = session.advanced.attachments.get(id2, "test.png")
            self.assertEqual(_IDENTIFIER_2, a2.details.remote_parameters.identifier)
            self.assertEqual(
                at2.replace(microsecond=0, tzinfo=None), a2.details.remote_parameters.at.replace(microsecond=0)
            )


if __name__ == "__main__":
    unittest.main()
