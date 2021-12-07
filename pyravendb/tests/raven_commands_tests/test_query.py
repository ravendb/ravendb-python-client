from pyravendb.data.query import IndexQuery
from pyravendb.documents.commands import PutDocumentCommand
from pyravendb.documents.indexes import IndexDefinition
from pyravendb.documents.operations.indexes import PutIndexesOperation
from pyravendb.raven_operations.query_operation import QueryCommand
from pyravendb.tests.test_base import TestBase
from pyravendb.data.document_conventions import DocumentConventions
import time


class TestQuery(TestBase):
    def setUp(self):
        super(TestQuery, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute_command(
            PutDocumentCommand(
                "products/10",
                None,
                {
                    "Name": "test",
                    "@metadata": {
                        "@collection": "Products",
                        "Raven-Python-Type": "object",
                    },
                },
            )
        )
        i = IndexDefinition()
        i.name = "AllDocuments"
        i.maps = self.index_map
        self.store.maintenance.send(PutIndexesOperation(i))

    def tearDown(self):
        super(TestQuery, self).tearDown()
        self.delete_all_topology_files()

    def test_only_query(self):
        query_command = QueryCommand(
            index_query=IndexQuery(
                query="From INDEX 'AllDocuments' WHERE Tag='Products'",
                wait_for_non_stale_results=True,
            ),
            conventions=DocumentConventions(),
        )
        self.requests_executor.execute_command(query_command)
        response = query_command.result
        self.assertEqual(response["Results"][0]["Name"], "test")

    def test_get_only_metadata(self):
        query_command = QueryCommand(
            index_query=IndexQuery(
                query="From INDEX 'AllDocuments' WHERE Tag='Products'",
                wait_for_non_stale_results=True,
            ),
            conventions=DocumentConventions(),
            metadata_only=True,
        )
        self.requests_executor.execute_command(query_command)
        response = query_command.result
        self.assertFalse("Name" in response["Results"][0])

    def test_get_only_index_entries(self):
        query_command = QueryCommand(
            index_query=IndexQuery(
                query="FROM INDEX 'AllDocuments' WHERE Tag='products'",
                wait_for_non_stale_results=True,
            ),
            conventions=DocumentConventions(),
            index_entries_only=True,
        )

        start = time.time()
        while True and time.time() - start < 30:
            self.requests_executor.execute_command(query_command)
            response = query_command.result
            if not response["IsStale"]:
                break
        self.assertFalse("@metadata" in response["Results"][0])

    def test_fail_with_no_index(self):
        query_command = QueryCommand(
            index_query=IndexQuery(
                query="From INDEX IndexIsNotExists WHERE Tag='products'",
                wait_for_non_stale_results=True,
            ),
            conventions=DocumentConventions(),
        )
        self.requests_executor.execute_command(query_command)
        self.assertIsNone(query_command.result)

    def test_fail_with_wrong_query(self):
        query_command = QueryCommand(
            index_query=IndexQuery(
                query="From INDEX IndexIsNotExists WHERE Tag=products",
                wait_for_non_stale_results=True,
            ),
            conventions=DocumentConventions(),
        )
        self.requests_executor.execute_command(query_command)
        self.assertIsNone(query_command.result)
