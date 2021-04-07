import unittest

from pyravendb.commands.raven_commands import (
    PutDocumentCommand,
    GetDocumentsByPrefixCommand,
)
from pyravendb.tests.test_base import TestBase


class TestGetDocumentsByPrefixCommand(TestBase):
    def setUp(self):
        super(TestGetDocumentsByPrefixCommand, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute(
            PutDocumentCommand("products/MB-200", {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute(
            PutDocumentCommand("products/MB-201", {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute(
            PutDocumentCommand("products/MB-100", {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute(
            PutDocumentCommand("products/MB-101", {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute(
            PutDocumentCommand("products/BM-100", {"Name": "test", "@metadata": {}})
        )
        self.requests_executor.execute(
            PutDocumentCommand("products/BM-200", {"Name": "test", "@metadata": {}})
        )

    def tearDown(self):
        super(TestGetDocumentsByPrefixCommand, self).tearDown()
        self.delete_all_topology_files()

    def test_init_WHEN_start_with_is_empty_THEN_raise(self):
        self.assertRaises(ValueError, GetDocumentsByPrefixCommand, None)

    def test_start_with(self):
        response = self.requests_executor.execute(
            GetDocumentsByPrefixCommand("products/MB")
        )
        self.assertEqual(len(response["Results"]), 4)

    def test_matches(self):
        response = self.requests_executor.execute(
            GetDocumentsByPrefixCommand("products/MB-", matches="2*")
        )
        self.assertEqual(len(response["Results"]), 2)

    def test_excludes(self):
        response = self.requests_executor.execute(
            GetDocumentsByPrefixCommand("products/BM-", exclude="2*")
        )
        self.assertEqual(len(response["Results"]), 1)

    def test_start_after(self):
        response = self.requests_executor.execute(
            GetDocumentsByPrefixCommand(
                "products/MB-", matches="2*", start_after="products/MB-200"
            )
        )
        self.assertEqual(len(response["Results"]), 1)

    def test_page(self):
        response = self.requests_executor.execute(
            GetDocumentsByPrefixCommand("products/MB", start=1, page_size=2)
        )
        self.assertEqual(len(response["Results"]), 2)


if __name__ == "__main__":
    unittest.main()
