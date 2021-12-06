import unittest

from pyravendb.documents.commands import PutDocumentCommand, GetDocumentsCommand
from pyravendb.tests.test_base import TestBase


class TestGetDocumentsByPrefixCommand(TestBase):
    def setUp(self):
        super(TestGetDocumentsByPrefixCommand, self).setUp()
        self.requests_executor = self.store.get_request_executor()

        self.requests_executor.execute_command(
            PutDocumentCommand("products/MB-200", None, {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute_command(
            PutDocumentCommand("products/MB-201", None, {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute_command(
            PutDocumentCommand("products/MB-100", None, {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute_command(
            PutDocumentCommand("products/MB-101", None, {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute_command(
            PutDocumentCommand("products/BM-100", None, {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute_command(
            PutDocumentCommand("products/BM-200", None, {"Name": "test", "@metadata": {}})
        )

    # todo: WIP

    def tearDown(self):
        super(TestGetDocumentsByPrefixCommand, self).tearDown()
        self.delete_all_topology_files()

    def test_start_with(self):
        with self.store.open_session() as session:
            response = session.load_starting_with(dict, "products/MB-")
            self.assertEqual(len(response), 4)

    def test_matches(self):
        response = self.requests_executor.execute_command(
            GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(starts_with="products/MB-", matches="2*")
            )
        )
        self.assertEqual(len(response["Results"]), 2)

    def test_excludes(self):
        response = self.requests_executor.execute_command(
            GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(starts_with="products/BM-", exclude="2*")
            )
        )
        self.assertEqual(len(response["Results"]), 1)

    def test_start_after(self):
        response = self.requests_executor.execute_command(
            GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(
                    starts_with="products/MB-", matches="2*", start_after="products/MB-200"
                )
            )
        )
        self.assertEqual(len(response["Results"]), 1)

    def test_page(self):
        response = self.requests_executor.execute_command(
            GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(
                    starts_with="products/MB", start=1, page_size=2
                )
            )
        )
        self.assertEqual(len(response["Results"]), 2)


if __name__ == "__main__":
    unittest.main()
