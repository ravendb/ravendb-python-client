import unittest
from pyravendb.data.indexes import IndexDefinition
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation, GetIndexOperation
from pyravendb.commands.raven_commands import DeleteIndexCommand
from pyravendb.tests.test_base import TestBase


class TestIndexActions(TestBase):
    def setUp(self):
        super(TestIndexActions, self).setUp()
        self.requests_executor = self.store.get_request_executor()
        self.conventions = self.store.conventions

    def tearDown(self):
        super(TestIndexActions, self).tearDown()
        self.delete_all_topology_files()

    def test_put_index_success(self):
        index = IndexDefinition(name="region", maps=self.index_map)
        assert self.requests_executor.execute(PutIndexesOperation(index).get_command(self.conventions))

    def test_get_index_success(self):
        index = IndexDefinition(name="get_index", maps=self.index_map)
        assert self.requests_executor.execute(PutIndexesOperation(index).get_command(self.conventions))
        self.assertIsNotNone(
            self.requests_executor.execute(GetIndexOperation("get_index").get_command(self.conventions)))

    def test_get_index_fail(self):
        self.assertIsNone(self.requests_executor.execute(GetIndexOperation("get_index").get_command(self.conventions)))

    def test_delete_index_success(self):
        index = IndexDefinition(name="delete", maps=self.index_map)
        assert self.requests_executor.execute(PutIndexesOperation(index).get_command(self.conventions))
        self.assertIsNone(self.requests_executor.execute(DeleteIndexCommand("delete")))

    def test_delete_index_fail(self):
        with self.assertRaises(ValueError):
            self.requests_executor.execute(DeleteIndexCommand(None))


if __name__ == "__main__":
    unittest.main()
