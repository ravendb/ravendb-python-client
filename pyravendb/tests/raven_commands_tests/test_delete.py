import unittest
from pyravendb.commands.raven_commands import *
from pyravendb.custom_exceptions import exceptions
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestDelete, cls).setUpClass()
        cls.response = cls.requests_executor.execute(
            PutDocumentCommand("products/101", {"Name": "test", "@metadata": {}}))
        cls.requests_executor.execute(PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}}))
        cls.other_response = cls.requests_executor.execute(
            PutDocumentCommand("products/102", {"Name": "test", "@metadata": {}}))

    def test_delete_success_no_etag(self):
        delete_command = DeleteDocumentCommand("products/10")
        self.assertIsNone(self.requests_executor.execute(delete_command))

    def test_delete_success_with_etag(self):
        delete_command = DeleteDocumentCommand("products/102", self.other_response["ETag"])
        self.assertIsNone(self.requests_executor.execute(delete_command))

    def test_delete_fail(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            self.requests_executor.execute(DeleteDocumentCommand("products/101", self.response["ETag"] + 10))

    if __name__ == "__main__":
        unittest.main()
