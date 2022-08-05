import os
import unittest

from ravendb.documents.operations.attachments import PutAttachmentOperation, AttachmentRequest
from ravendb.tests.test_base import TestBase, User


class TestAttachmentsStream(TestBase):
    def setUp(self):
        super(TestAttachmentsStream, self).setUp()

    @unittest.skip("Get many attachments in single request https://issues.hibernatingrhinos.com/issue/RDBC-427")
    def test_can_get_one_attachment_1(self):
        self.__can_get_one_attachment(1024)

    @unittest.skip("Get many attachments in single request https://issues.hibernatingrhinos.com/issue/RDBC-427")
    def test_can_get_one_attachment_2(self):
        self.__can_get_one_attachment(1024 * 1024)

    @unittest.skip("Get many attachments in single request https://issues.hibernatingrhinos.com/issue/RDBC-427")
    def test_can_get_one_attachment_3(self):
        self.__can_get_one_attachment(128 * 1024 * 1024)

    def __can_get_one_attachment(self, size: int) -> None:
        stream = os.urandom(size)
        key = "users/1-A"
        attachment_name = "Typical attachment name"

        with self.store.open_session() as session:
            user = User("su")
            session.store(user, key)
            session.save_changes()

        self.store.operations.send(PutAttachmentOperation(key, attachment_name, stream, "application/zip"))

        with self.store.open_session() as session:
            user = session.load(key, User)
            attachment_names = [AttachmentRequest(key, x.name) for x in session.advanced.attachments.get_names(user)]

            with session.advanced.attachments.get(attachment_names) as attachments_result:
                while attachments_result.has_next():
                    item = attachments_result.next()
                    self.assertEqual(stream, item.stream)
