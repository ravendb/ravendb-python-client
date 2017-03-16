import unittest
from pyravendb.data.indexes import IndexDefinition
from pyravendb.custom_exceptions import exceptions
from pyravendb.d_commands.raven_commands import PutIndexesCommand, GetIndexCommand, DeleteIndexCommand
from pyravendb.tests.test_base import TestBase


class TestIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestIndexActions, cls).setUpClass()

    def test_put_index_success(self):
        index = IndexDefinition(name="region", index_map=self.index_map)
        assert self.requests_executor.execute(PutIndexesCommand(index))

    def test_get_index_success(self):
        index = IndexDefinition(name="get_index", index_map=self.index_map)
        assert self.requests_executor.execute(PutIndexesCommand(index))
        self.assertIsNotNone(self.requests_executor.execute(GetIndexCommand("get_index")))

    def test_get_index_fail(self):
        self.assertIsNone(self.requests_executor.execute(GetIndexCommand("reg")))

    def test_delete_index_success(self):
        index = IndexDefinition(name="delete", index_map=self.index_map)
        assert self.requests_executor.execute(PutIndexesCommand(index))
        self.assertIsNone(self.requests_executor.execute(DeleteIndexCommand("delete")))

    def test_delete_index_fail(self):
        with self.assertRaises(ValueError):
            self.requests_executor.execute(DeleteIndexCommand(None))


if __name__ == "__main__":
    unittest.main()
