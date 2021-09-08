from pyravendb.raven_operations.maintenance_operations import (
    PutIndexesOperation,
    GetIndexNamesOperation,
    DeleteIndexOperation,
    GetIndexingStatusOperation,
    DisableIndexOperation,
    EnableIndexOperation,
)
from pyravendb.data.indexes import IndexDefinition, IndexingStatus

from pyravendb.tests.test_base import TestBase, UserWithId


class UsersIndex(IndexDefinition):
    def __init__(self, name="UsersByName"):
        maps = "from user in docs.userwithids select new { user.name }"
        super(UsersIndex, self).__init__(name, maps)


class TestIndexOperation(TestBase):
    def setUp(self):
        super(TestIndexOperation, self).setUp()

    def test_can_delete_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        names = self.store.maintenance.send(GetIndexNamesOperation(0, 10))
        self.assertEqual(2, len(names))  # @all_docs are second
        self.assertIn("UsersByName", names)

        self.store.maintenance.send(DeleteIndexOperation("UsersByName"))
        names = self.store.maintenance.send(GetIndexNamesOperation(0, 10))

        self.assertEqual(1, len(names))  # just @all_docs

    def test_can_disable_and_enable_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(DisableIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        index_status = indexing_status["Indexes"][0]
        self.assertEqual(str(IndexingStatus.disabled), index_status["Status"])

        self.store.maintenance.send(EnableIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        self.assertEqual(str(IndexingStatus.running), indexing_status["Status"])

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        index_status = indexing_status["Indexes"][0]
        self.assertEqual(str(IndexingStatus.running), index_status["Status"])
