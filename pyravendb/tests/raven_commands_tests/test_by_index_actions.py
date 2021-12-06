import unittest
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.custom_exceptions import exceptions
from pyravendb.documents.commands import PutDocumentCommand
from pyravendb.documents.indexes import IndexDefinition
from pyravendb.documents.operations.indexes import PutIndexesOperation
from pyravendb.tests.test_base import TestBase


class TestByIndexActions(TestBase):
    def setUp(self):
        super(TestByIndexActions, self).setUp()
        index_map = "from doc in docs.Testings " "select new{" "Name = doc.Name," "DocNumber = doc.DocNumber} "

        self.index_sort = IndexDefinition()

        self.index_sort.name = "Testing_Sort"
        self.index_sort.maps = index_map

        self.patch = "this.Name = 'Patched';"
        self.store.maintenance.send(PutIndexesOperation(self.index_sort))
        self.requests_executor = self.store.get_request_executor()
        for i in range(100):
            put_command = PutDocumentCommand(
                "testing/" + str(i),
                None,
                {
                    "Name": "test" + str(i),
                    "DocNumber": i,
                    "@metadata": {"@collection": "Testings"},
                },
            )
            self.requests_executor.execute_command(put_command)

    def tearDown(self):
        super(TestByIndexActions, self).tearDown()
        self.delete_all_topology_files()

    def test_update_by_index_success(self):
        query_command = QueryCommand(
            index_query=IndexQuery(query="From INDEX 'Testing_Sort'", wait_for_non_stale_results=True),
            conventions=DocumentConventions(),
        )
        self.requests_executor.execute(query_command)

        patch_command = PatchByQueryOperation(
            "From INDEX 'Testing_Sort' Update {{{0}}}".format(self.patch),
            options=QueryOperationOptions(allow_stale=False),
        ).get_command(self.store, self.store.conventions)
        result = self.requests_executor.execute(patch_command)
        result = self.store.operations.wait_for_operation_complete(result["operation_id"])
        self.assertIsNotNone(result)
        self.assertTrue(result["Result"]["Total"] >= 50)

    def test_update_by_index_fail(self):
        patch_command = PatchByQueryOperation(
            IndexQuery("From INDEX 'TeSort' Update {{{0}}}".format(self.patch)),
            options=QueryOperationOptions(allow_stale=False),
        ).get_command(self.store, self.store.conventions)
        with self.assertRaises(exceptions.InvalidOperationException):
            response = self.requests_executor.execute(patch_command)
            self.store.operations.wait_for_operation_complete(response["operation_id"])

    def test_delete_by_index_fail(self):
        delete_by_index_command = DeleteByQueryOperation("From Index 'region_2' WHERE Name = 'Western'").get_command(
            self.store, self.store.conventions
        )
        with self.assertRaises(exceptions.InvalidOperationException):
            response = self.requests_executor.execute_command(delete_by_index_command)
            self.assertIsNotNone(response)
            self.store.operations.wait_for_operation_complete(response["operation_id"])

    def test_delete_by_index_success(self):
        query_command = QueryCommand(
            self.store.conventions,
            IndexQuery(
                query="FROM INDEX 'Testing_Sort' WHERE DocNumber BETWEEN '0' AND '49'",
                wait_for_non_stale_results=True,
            ),
        )
        self.requests_executor.execute(query_command)
        delete_by_index_command = DeleteByQueryOperation(
            "FROM INDEX 'Testing_Sort' WHERE DocNumber BETWEEN '0' AND '49'",
            options=QueryOperationOptions(allow_stale=False),
        ).get_command(self.store, self.store.conventions)
        response = self.requests_executor.execute(delete_by_index_command)
        result = self.store.operations.wait_for_operation_complete(response["operation_id"])
        assert result["Status"] == "Completed"


if __name__ == "__main__":
    unittest.main()
