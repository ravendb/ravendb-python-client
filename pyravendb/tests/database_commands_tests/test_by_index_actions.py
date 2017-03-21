import unittest

from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery
from pyravendb.data.patches import ScriptedPatchRequest
from pyravendb.tests.test_base import TestBase
from pyravendb.data.operations import Operations
from pyravendb.data.indexes import IndexDefinition

class TestByIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestByIndexActions, cls).setUpClass()
        cls.patch = ScriptedPatchRequest("this.Name = 'testing';")
        cls.db.put("testing/1", {"Name": "test"}, {"Raven-Entity-Name": "Testing"})
        cls.operation = Operations(cls.request_handler)
        index_map =("from doc in docs.Testing "
                    "select new{"
                    "Name = doc.Name}")
        index = IndexDefinition(index_map)
        cls.db.put_index("indexOperation", index, True)

    def test_update_by_index_success(self):
        result = self.db.update_by_index("indexOperation", IndexQuery("Name:test"), self.patch)
        status = self.operation.wait_for_operation_complete(result["OperationId"])
        self.assertTrue(status["Completed"] == True and status["Faulted"] == False)



    def test_update_by_index_fail(self):
        with self.assertRaises(exceptions.InvalidOperationException):
            result = self.db.update_by_index("c", IndexQuery("Name:test"), self.patch)
            self.operation.wait_for_operation_complete(result["OperationId"])

    def test_delete_by_index_fail(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            self.db.delete_by_index("region2", IndexQuery("Name:Western"))

    def test_delete_by_index_success(self):
        result = self.db.delete_by_index("indexOperation", IndexQuery("Name:test"))
        status = self.operation.wait_for_operation_complete(result["OperationId"])
        self.assertTrue(status["Completed"] == True and status["Faulted"] == False)


if __name__ == "__main__":
    unittest.main()
