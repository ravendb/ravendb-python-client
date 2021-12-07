import unittest
from pyravendb.custom_exceptions import exceptions
from pyravendb.documents.commands import PutDocumentCommand, DeleteDocumentCommand
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    def setUp(self):
        super(TestDelete, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        command = PutDocumentCommand("products/101", None, {"Name": "test", "@metadata": {}})
        self.requests_executor.execute_command(command)
        self.response = command.result

        self.requests_executor.execute_command(
            PutDocumentCommand("products/10", None, {"Name": "test", "@metadata": {}})
        )
        other_command = PutDocumentCommand("products/102", None, {"Name": "test", "@metadata": {}})
        self.requests_executor.execute_command(other_command)
        self.other_response = other_command.result

    def tearDown(self):
        super(TestDelete, self).tearDown()
        self.delete_all_topology_files()

    def test_delete_success_no_change_vector(self):
        delete_command = DeleteDocumentCommand("products/10")
        self.requests_executor.execute_command(delete_command)
        self.assertIsNone(delete_command.result)

    def test_delete_success_with_change_vector(self):
        delete_command = DeleteDocumentCommand("products/102", self.other_response.change_vector)
        self.requests_executor.execute_command(delete_command)
        self.assertIsNone(delete_command.result)

    def test_delete_fail(self):
        with self.assertRaises(Exception):
            self.requests_executor.execute_command(
                DeleteDocumentCommand("products/101", self.response["ChangeVector"] + "10")
            )

    if __name__ == "__main__":
        unittest.main()
