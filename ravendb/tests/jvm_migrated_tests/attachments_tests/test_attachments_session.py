from ravendb import constants
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.documents.commands.batches import DeleteCommandData
from ravendb.documents.operations.attachments import DeleteAttachmentOperation
from ravendb.tests.test_base import TestBase, User


class TestAttachmentsSession(TestBase):
    def setUp(self):
        super(TestAttachmentsSession, self).setUp()

    def test_put_attachments(self):
        names = ["profile.png", "background-photo.jpg", "fileNAME_#$1^%_בעברית.txt"]

        with self.store.open_session() as session:
            profile_stream = bytes([1, 2, 3])
            background_stream = bytes([10, 20, 30, 40, 50])
            file_stream = bytes([1, 2, 3, 4, 5])

            user = User("Fitzchak")
            session.store(user, "users/1")

            session.advanced.attachments.store("users/1", names[0], profile_stream, "image/png")
            session.advanced.attachments.store(user, names[1], background_stream, "ImGge/jPeG")
            session.advanced.attachments.store(user, names[2], file_stream)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            metadata = session.get_metadata_for(user)
            self.assertEqual(
                "HasAttachments",
                metadata.get(
                    constants.Documents.Metadata.FLAGS,
                ),
            )

            attachments = metadata.get(
                constants.Documents.Metadata.ATTACHMENTS,
            )

            self.assertEqual(3, len(attachments))

            ordered_names = names
            ordered_names.sort()

            for i in range(len(names)):
                name = ordered_names[i]
                attachment = attachments[i]
                self.assertEqual(attachment["Name"], name)

    def test_put_document_and_attachment_and_delete_should_throw(self):
        with self.store.open_session() as session:
            profile_stream = bytes([1, 2, 3])

            user = User("Fitzchak")
            session.store(user, "users/1")

            session.advanced.attachments.store(user, "profile.png", profile_stream, "image/png")

            session.delete(user)

            with self.assertRaises(RuntimeError):
                session.save_changes()

    def test_get_attachment_names(self):
        names = ["profile.png"]

        with self.store.open_session() as session:
            profile_stream = bytes([1, 2, 3])

            user = User("Fitzchak")
            session.store(user, "users/1")

            session.advanced.attachments.store("users/1", names[0], profile_stream, "image/png")

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            attachments = session.advanced.attachments.get_names(user)

            self.assertEqual(1, len(attachments))

            attachment = attachments[0]

            self.assertEqual("image/png", attachment.content_type)

            self.assertEqual(names[0], attachment.name)

            self.assertEqual(3, attachment.size)

    def test_delete_document_by_command_and_than_its_attachments__this_is_no_op_but_should_be_supported(self):
        with self.store.open_session() as session:
            user = User("Fitzchak")
            session.store(user, "users/1")

            stream = bytes([1, 2, 3])
            session.advanced.attachments.store(user, "file", stream, "image/png")
            session.save_changes()

        with self.store.open_session() as session:
            session.defer(DeleteCommandData("users/1", None))
            session.advanced.attachments.delete("users/1", "file")
            session.advanced.attachments.delete("users/1", "file")

            session.save_changes()

    def test_delete_attachments(self):
        with self.store.open_session() as session:
            user = User("Fitzchak")
            session.store(user, "users/1")

            stream1 = bytes([1, 2, 3])
            stream2 = bytes([1, 2, 3, 4, 5, 6])
            stream3 = bytes([1, 2, 3, 4, 5, 7, 8, 9])
            stream4 = bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

            session.advanced.attachments.store(user, "file1", stream1, "image/png")
            session.advanced.attachments.store(user, "file2", stream2, "image/png")
            session.advanced.attachments.store(user, "file3", stream3, "image/png")
            session.advanced.attachments.store(user, "file4", stream4, "image/png")

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)

            with session.advanced.attachments.get("users/1", "file2") as attachment_results:
                self.assertEqual("file2", attachment_results.details.name)

            session.advanced.attachments.delete("users/1", "file2")
            session.advanced.attachments.delete(user, "file4")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            metadata = session.get_metadata_for(user)
            self.assertEqual(
                "HasAttachments",
                metadata.get(
                    constants.Documents.Metadata.FLAGS,
                ),
            )

            attachments = metadata.get(
                constants.Documents.Metadata.ATTACHMENTS,
            )

            self.assertEqual(2, len(attachments))

            result = session.advanced.attachments.get("users/1", "file1")
            file1bytes = bytes(result.data)
            self.assertEqual(3, len(file1bytes))

    def test_attachment_exists(self):
        with self.store.open_session() as session:
            stream = bytes([1, 2, 3])
            user = User("Fitzchak")
            session.store(user, "users/1")
            session.advanced.attachments.store("users/1", "profile", stream, "image/png")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertTrue(session.advanced.attachments.exists("users/1", "profile"))
            self.assertFalse(session.advanced.attachments.exists("users/1", "background-photo"))
            self.assertFalse(session.advanced.attachments.exists("users/2", "profile"))

    def test_throw_when_two_attachments_with_the_same_name_in_session(self):
        with self.store.open_session() as session:
            stream = bytes([1, 2, 3])
            stream2 = bytes([1, 2, 3, 4, 5])
            user = User("Fitzchak")
            session.store(user, "users/1")

            session.advanced.attachments.store(user, "profile", stream, "image/png")

            with self.assertRaises(InvalidOperationException):
                session.advanced.attachments.store(user, "profile", stream2)

    def test_delete_document_and_than_its_attachments__this_is_no_op_but_should_be_supported(self):
        with self.store.open_session() as session:
            user = User("Fitzchak")
            session.store(user, "users/1")
            stream = bytes([1, 2, 3])

            session.advanced.attachments.store(user, "file", stream, "image/png")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)

            session.delete(user)
            session.advanced.attachments.delete(user, "file")
            session.advanced.attachments.delete(user, "file")  # this should be no-op

            session.save_changes()

    def test_throw_if_stream_is_use_twice(self):
        with self.store.open_session() as session:
            stream = bytes([1, 2, 3])

            user = User("Fitzchak")
            session.store(user, "users/1")

            session.advanced.attachments.store(user, "profile", stream, "image/png")
            session.advanced.attachments.store(user, "other", stream)

            with self.assertRaises(RuntimeError):
                session.save_changes()

    def test_get_attachment_releases_resources(self):
        count = 30

        with self.store.open_session() as session:
            user = User()
            session.store(user, "users/1")
            session.save_changes()

            for i in range(count):
                with self.store.open_session() as session:
                    stream1 = bytes([1, 2, 3])
                    session.advanced.attachments.store("users/1", f"file{i}", stream1, "image/png")
                    session.save_changes()

            for i in range(count):
                with self.store.open_session() as session:
                    with session.advanced.attachments.get("users/1", f"file{i}") as result:
                        # dont' read data as it marks entity as consumed
                        pass

    def test_delete_attachment_using_command(self):
        with self.store.open_session() as session:
            user = User("Fitzchak")
            session.store(user, "users/1")

            stream1 = bytes([1, 2, 3])
            stream2 = bytes([1, 2, 3, 4, 5, 6])

            session.advanced.attachments.store(user, "file1", stream1, "image/png")
            session.advanced.attachments.store(user, "file2", stream2, "image/png")

            session.save_changes()

        self.store.operations.send(DeleteAttachmentOperation("users/1", "file2"))

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            metadata = session.get_metadata_for(user)
            self.assertEqual(
                "HasAttachments",
                str(
                    metadata.get(
                        constants.Documents.Metadata.FLAGS,
                    )
                ),
            )

            attachments = metadata.get(
                constants.Documents.Metadata.ATTACHMENTS,
            )

            self.assertEqual(1, len(attachments))

            result = session.advanced.attachments.get("users/1", "file1")
            file1bytes = bytes(result.data)
            self.assertEqual(3, len(file1bytes))
