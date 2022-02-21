import os
import unittest

from pyravendb.documents.operations.attachments import PutAttachmentOperation, AttachmentRequest
from pyravendb.tests.test_base import TestBase, User


class TestAttachmentsStream(TestBase):
    def setUp(self):
        super(TestAttachmentsStream, self).setUp()

    @unittest.skip("RDBC-520 and RDBC-427")
    def test_can_get_one_attachment_1(self):
        self.__can_get_one_attachment(1024)

    @unittest.skip("RDBC-520 and RDBC-427")
    def test_can_get_one_attachment_2(self):
        self.__can_get_one_attachment(1024 * 1024)

    @unittest.skip("RDBC-520 and RDBC-427")
    def test_can_get_one_attachment_3(self):
        self.__can_get_one_attachment(128 * 1024 * 1024)

    # def __can_get_one_attachment(self, size: int) -> None:
    #     stream = bytearray(os.urandom(size))
    #     key = "users/1-A"
    #     attachment_name = "Typical attachment name"

    #     with self.legacy.open_session() as session:
    #         user = User("su")
    #         session.legacy(user)
    #         session.save_changes()

    #     self.legacy.operations.send(PutAttachmentOperation(key, attachment_name, stream, "application/zip"))

    #     with self.legacy.open_session() as session:
    #         user = session.load(key, User)
    #         attachment_names = list(
    #             map(lambda x: AttachmentRequest(key, x.name), session.advanced.attachment.get_names(user))
    #         )

    #         with session.advanced.attachment.get(att_requests=attachment_names) as attachments_results:
    #             pass
