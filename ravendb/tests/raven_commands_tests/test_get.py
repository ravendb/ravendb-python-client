import unittest
from ravendb.documents.commands.crud import PutDocumentCommand, GetDocumentsCommand
from ravendb.tests.test_base import TestBase


class TestGet(TestBase):
    def setUp(self):
        super(TestGet, self).setUp()
        put_command = PutDocumentCommand("products/101", None, {"Name": "test", "@metadata": {}})
        put_command2 = PutDocumentCommand("products/10", None, {"Name": "test", "@metadata": {}})
        put_command3 = PutDocumentCommand("products/102", None, {"Name": "test", "@metadata": {}})
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute_command(put_command)
        self.requests_executor.execute_command(put_command2)
        self.requests_executor.execute_command(put_command3)

        command = GetDocumentsCommand(GetDocumentsCommand.GetDocumentsByIdCommandOptions("products/101"))
        self.requests_executor.execute_command(command)
        self.response = command.result

        other_command = GetDocumentsCommand(GetDocumentsCommand.GetDocumentsByIdCommandOptions("products/10"))
        self.requests_executor.execute_command(other_command)
        self.other_response = other_command.result

        paged_command = GetDocumentsCommand(GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(2, 1))
        self.requests_executor.execute_command(paged_command)
        self.paged_response = paged_command.result

    def tearDown(self):
        super(TestGet, self).tearDown()
        self.delete_all_topology_files()

    def test_equal(self):
        self.assertEqual(self.response.results[0]["@metadata"]["@id"], "products/101")

    def test_not_equal(self):
        self.assertNotEqual(
            self.response.results[0]["@metadata"]["@id"],
            self.other_response.results[0]["@metadata"]["@id"],
        )

    def test_paging(self):
        self.assertEqual(len(self.paged_response.results), 1)
        self.assertEqual(self.paged_response.results[0]["@metadata"]["@id"], "products/101")  # todo: check if valid

    def test_null(self):
        command = GetDocumentsCommand(GetDocumentsCommand.GetDocumentsByIdCommandOptions("product"))
        self.requests_executor.execute_command(command)
        self.assertIsNone(command.result)


if __name__ == "__main__":
    unittest.main()
