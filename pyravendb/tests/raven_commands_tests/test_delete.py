import unittest
from pyravendb.commands.raven_commands import *
from pyravendb.custom_exceptions import exceptions
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    def setUp(self):
        super(TestDelete, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        self.response = self.requests_executor.execute(
            PutDocumentCommand("products/101", {"Name": "test", "@metadata": {}}))

        self.requests_executor.execute(PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}}))
        self.other_response = self.requests_executor.execute(
            PutDocumentCommand("products/102", {"Name": "test", "@metadata": {}}))

    def tearDown(self):
        super(TestDelete, self).tearDown()
        self.delete_all_topology_files()

    def test_delete_success_no_change_vector(self):
        delete_command = DeleteDocumentCommand("products/10")
        self.assertIsNone(self.requests_executor.execute(delete_command))

    def test_delete_success_with_change_vector(self):
        delete_command = DeleteDocumentCommand("products/102", self.other_response["ChangeVector"])
        self.assertIsNone(self.requests_executor.execute(delete_command))

    def test_delete_fail(self):
        with self.assertRaises(Exception):
            self.requests_executor.execute(DeleteDocumentCommand("products/101", self.response["ChangeVector"] + '10'))

    if __name__ == "__main__":
        unittest.main()
