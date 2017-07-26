import unittest
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.d_commands.raven_commands import *
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery, IndexFieldOptions, SortOptions
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase
from pyravendb.data.operation import OperationExecutor, QueryOperationOptions


class TestByIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestByIndexActions, cls).setUpClass()
        index_map = ("from doc in docs.Testings "
                     "select new{"
                     "Name = doc.Name,"
                     "DocNumber = doc.DocNumber} ")

        cls.index_sort = IndexDefinition(name="Testing_Sort", index_map=index_map,
                                         fields={"DocNumber": IndexFieldOptions(sort_options=SortOptions.numeric)})
        cls.patch = PatchRequest("this.Name = 'Patched';")
        cls.requests_executor.execute(PutIndexesCommand(cls.index_sort))
        cls.operations = OperationExecutor(cls.requests_executor)
        for i in range(100):
            put_command = PutDocumentCommand("testing/" + str(i),
                                             {"Name": "test" + str(i), "DocNumber": i,
                                              "@metadata": {"@collection": "Testings"}})
            cls.requests_executor.execute(put_command)

    def test_update_by_index_success(self):
        query_command = QueryCommand("Testing_Sort", IndexQuery(query="Name:*", wait_for_non_stale_results=True),
                                     conventions=DocumentConvention())
        self.requests_executor.execute(query_command)
        patch_by_index_command = PatchByIndexCommand("Testing_Sort", IndexQuery("Name:*"), self.patch,
                                                     options=QueryOperationOptions(allow_stale=False))
        response = self.requests_executor.execute(patch_by_index_command)
        result = self.operations.wait_for_operation_complete(response["OperationId"])
        self.assertIsNotNone(result)
        self.assertTrue(result["Result"]["Total"] >= 50)

    def test_update_by_index_fail(self):
        patch_by_index_command = PatchByIndexCommand("", IndexQuery("Name:test"), self.patch)
        response = self.requests_executor.execute(patch_by_index_command)
        with self.assertRaises(exceptions.InvalidOperationException):
            self.requests_executor.execute(self.operations.wait_for_operation_complete(response["OperationId"]))

    def test_delete_by_index_fail(self):
        delete_by_index_command = DeleteByIndexCommand("region2", IndexQuery("Name:Western"))
        response = self.requests_executor.execute(delete_by_index_command)
        with self.assertRaises(exceptions.InvalidOperationException):
            self.requests_executor.execute(self.operations.wait_for_operation_complete(response["OperationId"]))

    def test_delete_by_index_success(self):
        query_command = QueryCommand("Testing_Sort",
                                     IndexQuery(query="DocNumber_D_Range:[0 TO 49]", wait_for_non_stale_results=True),
                                     conventions=DocumentConvention())
        self.requests_executor.execute(query_command)
        delete_by_index_command = DeleteByIndexCommand("Testing_Sort", IndexQuery("DocNumber_D_Range:[0 TO 49]"),
                                                       options=QueryOperationOptions(allow_stale=False))
        response = self.requests_executor.execute(delete_by_index_command)
        result = self.operations.wait_for_operation_complete(response["OperationId"])
        assert result["Status"] == "Completed"


if __name__ == "__main__":
    unittest.main()
