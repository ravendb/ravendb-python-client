import unittest

from pyravendb.d_commands.raven_commands import *
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestByIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestByIndexActions, cls).setUpClass()
        cls.patch = PatchRequest("this.Name = 'testing';")
        put_command = PutDocumentCommand("testing/1", {"Name": "test", "@metadata": {"Raven-Entity-Name": "Testing"}})
        cls.requests_executor.execute(put_command)

    def test_update_by_index_success(self):
        patch_by_index_command = PatchByIndexCommand("Testing", IndexQuery("Name:test"), self.patch)
        result = self.requests_executor.execute(patch_by_index_command)
        self.requests_executor.execute(GetOperationStateCommand(result["OperationId"]))

    def test_update_by_index_fail(self):
        patch_by_index_command = PatchByIndexCommand("", IndexQuery("Name:test"), self.patch)
        result = self.requests_executor.execute(patch_by_index_command)
        with self.assertRaises(exceptions.IndexDoesNotExistException):
            self.requests_executor.execute(GetOperationStateCommand(result["OperationId"]))



    def test_delete_by_index_fail(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            delete_by_index_command = DeleteByIndexCommand("region2", IndexQuery("Name:Western"))
            self.assertIsNotNone(self.requests_executor.execute(delete_by_index_command))

    def test_delete_by_index_success(self):
        delete_by_index_command = DeleteByIndexCommand("Testing", IndexQuery("Name:test"))
        self.assertIsNotNone(self.requests_executor.execute(delete_by_index_command))


if __name__ == "__main__":
    unittest.main()
