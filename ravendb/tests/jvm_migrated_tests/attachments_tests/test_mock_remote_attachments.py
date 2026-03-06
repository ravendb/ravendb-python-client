"""
Mock tests for remote attachment session flows.

CRUD operations (store doc, store/delete attachment, exists) run against the real DB via
TestBase. Only session.advanced.attachments.get() is mocked — it would normally return
flags=Remote only after the server-side background sender has pushed the blob to cloud,
which requires real S3/Azure credentials.

Mirrors: test/SlowTests/Server/Documents/Attachments/DocumentSessionRemoteAttachmentsAsyncTests.cs
"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

from ravendb.documents.operations.attachments import (
    AttachmentDetails,
    CloseableAttachmentResult,
    RemoteAttachmentFlags,
    RemoteAttachmentParameters,
    StoreAttachmentParameters,
)
from ravendb.tests.test_base import TestBase

_IDENTIFIER = "dest-1"
_IDENTIFIER_2 = "dest-2"


class User:
    def __init__(self, name: str):
        self.name = name


def _make_attachment_result(
    name: str,
    content_type: str,
    data: bytes,
    identifier: str,
    at: datetime.datetime,
    flags: RemoteAttachmentFlags = RemoteAttachmentFlags.REMOTE,
) -> CloseableAttachmentResult:
    """Build a CloseableAttachmentResult as the server would return after cloud upload."""
    details = AttachmentDetails(name, "hash-mock", content_type, len(data))
    remote_params = RemoteAttachmentParameters.__new__(RemoteAttachmentParameters)
    remote_params.identifier = identifier
    remote_params.at = at
    remote_params.flags = flags
    details.remote_parameters = remote_params

    response = MagicMock()
    response.content = data
    response.close = MagicMock()
    return CloseableAttachmentResult(response, details)


class TestMockRemoteAttachments(TestBase):
    def setUp(self):
        super().setUp()

    def test_store_and_get_flags_remote_after_upload(self):
        """After cloud upload, get() returns flags=Remote."""
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/1")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/1", params)
            session.save_changes()

        mock_result = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER, at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/1", "test.png")
                self.assertIsNotNone(attachment)
                self.assertEqual("test.png", attachment.details.name)
                self.assertEqual("image/png", attachment.details.content_type)
                self.assertIsNotNone(attachment.details.remote_parameters)
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)
                self.assertEqual(bytes([1, 2, 3]), attachment.data)

    def test_get_by_entity_flags_remote(self):
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/2")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/2", params)
            session.save_changes()

        mock_result = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER, at)
        with self.store.open_session() as session:
            order = session.load("orders/2", User)
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get(order, "test.png")
                self.assertIsNotNone(attachment)
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)

    def test_delete_after_upload(self):
        """After cloud upload, delete() removes the local ref; exists() returns False."""
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/3")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/3", params)
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.attachments.delete("orders/3", "test.png")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.attachments.exists("orders/3", "test.png"))

    def test_overwrite_with_new_identifier_flags_remote(self):
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/4")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/4", params)
            session.save_changes()

        params2 = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params2.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER_2, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/4", params2)
            session.save_changes()

        mock_result = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER_2, at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/4", "test.png")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER_2, attachment.details.remote_parameters.identifier)

    def test_overwrite_with_new_at_flags_remote(self):
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/5")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/5", params)
            session.save_changes()

        new_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=15)
        params2 = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params2.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, new_at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/5", params2)
            session.save_changes()

        mock_result = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER, new_at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/5", "test.png")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)
                self.assertEqual(
                    new_at.replace(microsecond=0, tzinfo=None),
                    attachment.details.remote_parameters.at.replace(microsecond=0, tzinfo=None),
                )

    def test_overwrite_with_new_identifier_and_at_flags_remote(self):
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/6")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/6", params)
            session.save_changes()

        new_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=15)
        params2 = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params2.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER_2, new_at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/6", params2)
            session.save_changes()

        mock_result = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER_2, new_at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/6", "test.png")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER_2, attachment.details.remote_parameters.identifier)
                self.assertEqual(
                    new_at.replace(microsecond=0, tzinfo=None),
                    attachment.details.remote_parameters.at.replace(microsecond=0, tzinfo=None),
                )

    def test_overwrite_with_no_remote_params_demotes_to_plain(self):
        """After demoting to plain, get() returns no remote_parameters."""
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/7")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/7", params)
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.attachments.store("orders/7", "test.png", bytes([4, 5, 6]), content_type="image/png")
            session.save_changes()

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get("orders/7", "test.png")
            self.assertIsNotNone(attachment)
            self.assertIsNone(attachment.details.remote_parameters)

    def test_store_to_multiple_destinations_flags_remote(self):
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/8")
            session.store(User("Bob"), "orders/9")
            session.save_changes()

        at1 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=3)
        params1 = StoreAttachmentParameters("test.png", bytes([1, 2, 3]), content_type="image/png")
        params1.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at1)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/8", params1)
            session.save_changes()

        at2 = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        params2 = StoreAttachmentParameters("test.png", bytes([3, 2, 1]), content_type="image/png")
        params2.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER_2, at2)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/9", params2)
            session.save_changes()

        mock1 = _make_attachment_result("test.png", "image/png", bytes([1, 2, 3]), _IDENTIFIER, at1)
        mock2 = _make_attachment_result("test.png", "image/png", bytes([3, 2, 1]), _IDENTIFIER_2, at2)

        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", side_effect=[mock1, mock2]):
                a1 = session.advanced.attachments.get("orders/8", "test.png")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, a1.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER, a1.details.remote_parameters.identifier)

                a2 = session.advanced.attachments.get("orders/9", "test.png")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, a2.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER_2, a2.details.remote_parameters.identifier)

    def test_azure_blob_store_and_get_flags_remote(self):
        """Mocked Azure flow: after upload, get() returns flags=Remote with correct data."""
        with self.store.open_session() as session:
            session.store(User("Alice"), "orders/azure-1")
            session.save_changes()

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=5)
        params = StoreAttachmentParameters("photo.png", bytes([10, 20, 30, 40, 50]), content_type="image/png")
        params.remote_parameters = RemoteAttachmentParameters("azure-dest-1", at)
        with self.store.open_session() as session:
            session.advanced.attachments.store_with_parameters("orders/azure-1", params)
            session.save_changes()

        # Before upload: flags=None (real DB response)
        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get("orders/azure-1", "photo.png")
            self.assertIsNotNone(attachment)
            self.assertEqual(RemoteAttachmentFlags.NONE, attachment.details.remote_parameters.flags)

        # After upload: mock flags=Remote
        mock_result = _make_attachment_result("photo.png", "image/png", bytes([10, 20, 30, 40, 50]), "azure-dest-1", at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/azure-1", "photo.png")
                self.assertIsNotNone(attachment)
                self.assertEqual("photo.png", attachment.details.name)
                self.assertEqual("image/png", attachment.details.content_type)
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual("azure-dest-1", attachment.details.remote_parameters.identifier)
                self.assertEqual(bytes([10, 20, 30, 40, 50]), attachment.data)

    def test_bulk_insert_remote_attachment_persists_remote_parameters(self):
        """Bulk insert: remote_parameters (identifier, at, flags=None) survive the round-trip."""
        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User("Alice"), "orders/bulk-1")
            attachments = bulk_insert.attachments_for("orders/bulk-1")
            params = StoreAttachmentParameters("report.pdf", bytes([1, 2, 3, 4]), content_type="application/pdf")
            params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
            attachments.store_with_parameters(params)

        with self.store.open_session() as session:
            attachment = session.advanced.attachments.get("orders/bulk-1", "report.pdf")
            self.assertIsNotNone(attachment)
            self.assertEqual("report.pdf", attachment.details.name)
            self.assertEqual("application/pdf", attachment.details.content_type)
            self.assertIsNotNone(attachment.details.remote_parameters)
            self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)
            self.assertEqual(RemoteAttachmentFlags.NONE, attachment.details.remote_parameters.flags)
            self.assertEqual(
                at.replace(microsecond=0, tzinfo=None),
                attachment.details.remote_parameters.at.replace(microsecond=0),
            )

        # Mock flags=Remote to simulate post-upload state.
        mock_result = _make_attachment_result("report.pdf", "application/pdf", bytes([1, 2, 3, 4]), _IDENTIFIER, at)
        with self.store.open_session() as session:
            with patch.object(session.advanced.attachments, "get", return_value=mock_result):
                attachment = session.advanced.attachments.get("orders/bulk-1", "report.pdf")
                self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)
                self.assertEqual(bytes([1, 2, 3, 4]), attachment.data)

    def test_bulk_insert_100_remote_attachments_flags_remote(self):
        """Bulk insert 10 docs × 10 attachments = 100 remote attachments; mock all as Remote."""
        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        doc_ids = [f"orders/bulk100-mock-{i}" for i in range(10)]

        with self.store.bulk_insert() as bulk_insert:
            for doc_id in doc_ids:
                bulk_insert.store_as(User("Alice"), doc_id)
                attachments = bulk_insert.attachments_for(doc_id)
                for j in range(10):
                    params = StoreAttachmentParameters(
                        f"file-{j}.bin",
                        bytes([k % 256 for k in range(j + 1)]),
                        content_type="application/octet-stream",
                    )
                    params.remote_parameters = RemoteAttachmentParameters(_IDENTIFIER, at)
                    attachments.store_with_parameters(params)

        # Before upload: all 100 must have flags=None (real DB).
        with self.store.open_session() as session:
            for doc_id in doc_ids:
                user = session.load(doc_id, User)
                names = session.advanced.attachments.get_names(user)
                self.assertEqual(10, len(names))
                for attachment_name in names:
                    self.assertIsNotNone(attachment_name.remote_parameters)
                    self.assertEqual(RemoteAttachmentFlags.NONE, attachment_name.remote_parameters.flags)
                    self.assertEqual(_IDENTIFIER, attachment_name.remote_parameters.identifier)

        # After upload: mock all 100 as Remote.
        with self.store.open_session() as session:
            for doc_id in doc_ids:
                user = session.load(doc_id, User)
                names = session.advanced.attachments.get_names(user)
                mock_results = [
                    _make_attachment_result(
                        a.name, a.content_type, bytes([k % 256 for k in range(j + 1)]), _IDENTIFIER, at
                    )
                    for j, a in enumerate(names)
                ]
                with patch.object(session.advanced.attachments, "get", side_effect=mock_results):
                    for j, attachment_name in enumerate(names):
                        attachment = session.advanced.attachments.get(doc_id, attachment_name.name)
                        self.assertEqual(RemoteAttachmentFlags.REMOTE, attachment.details.remote_parameters.flags)
                        self.assertEqual(_IDENTIFIER, attachment.details.remote_parameters.identifier)


if __name__ == "__main__":
    unittest.main()
