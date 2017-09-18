from pyravendb.data.query import IndexQuery
from pyravendb.tests.test_base import TestBase
from pyravendb.commands.raven_commands import *
from pyravendb.custom_exceptions.exceptions import ErrorResponseException
from pyravendb.data.document_convention import DocumentConvention


class TestQuery(TestBase):
    def setUp(self):
        super(TestQuery, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute(
            PutDocumentCommand("products/10", {"Name": "test", "@metadata": {"@collection": "Products",
                                                                             "Raven-Python-Type": "object"}}))

    def tearDown(self):
        super(TestQuery, self).tearDown()
        self.delete_all_topology_files()

    def test_only_query(self):
        query_command = QueryCommand(
            index_query=IndexQuery(query="From INDEX 'AllDocuments' WHERE Tag='Products'",
                                   wait_for_non_stale_results=True),
            conventions=DocumentConvention())
        response = self.requests_executor.execute(query_command)
        self.assertEqual(response["Results"][0]["Name"], "test")

    def test_get_only_metadata(self):
        query_command = QueryCommand(
            index_query=IndexQuery(query="From INDEX 'AllDocuments' WHERE Tag='Products'",
                                   wait_for_non_stale_results=True), conventions=DocumentConvention(),
            metadata_only=True)
        response = self.requests_executor.execute(query_command)
        self.assertFalse("Name" in response["Results"][0])

    def test_get_only_index_entries(self):
        query_command = QueryCommand(index_query=IndexQuery(query="From INDEX 'AllDocuments' WHERE Tag='products'",
                                                            wait_for_non_stale_results=True),
                                     conventions=DocumentConvention(),
                                     index_entries_only=True)
        response = self.requests_executor.execute(query_command)
        self.assertFalse("@metadata" in response["Results"][0])

    def test_fail_with_no_index(self):
        query_command = QueryCommand(index_query=IndexQuery(query="From INDEX IndexIsNotExists WHERE Tag='products'",
                                                            wait_for_non_stale_results=True),
                                     conventions=DocumentConvention())
        self.assertIsNone(self.requests_executor.execute(query_command))

    def test_fail_with_wrong_query(self):
        with self.assertRaises(ErrorResponseException):
            query_command = QueryCommand(index_query=IndexQuery(query="From INDEX IndexIsNotExists WHERE Tag=products",
                                                                wait_for_non_stale_results=True),
                                         conventions=DocumentConvention())
            self.assertIsNone(self.requests_executor.execute(query_command))
