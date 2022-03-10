from ravendb.documents.indexes.definitions import IndexDefinition, IndexLockMode, IndexPriority
from ravendb.documents.operations.indexes import (
    PutIndexesOperation,
    GetIndexNamesOperation,
    DeleteIndexOperation,
    DisableIndexOperation,
    GetIndexingStatusOperation,
    IndexingStatus,
    EnableIndexOperation,
    IndexStatus,
    IndexRunningStatus,
    GetIndexStatisticsOperation,
    StopIndexOperation,
    StartIndexOperation,
    StopIndexingOperation,
    StartIndexingOperation,
    GetIndexesStatisticsOperation,
    GetIndexErrorsOperation,
    GetIndexesOperation,
    GetIndexOperation,
    SetIndexesLockOperation,
    SetIndexesPriorityOperation,
    GetTermsOperation,
    IndexHasChangedOperation,
)

from ravendb.tests.test_base import TestBase, UserWithId


class UsersIndex(IndexDefinition):
    def __init__(self, name="UsersByName"):
        maps = "from user in docs.userwithids select new { user.name }"
        super(UsersIndex, self).__init__()
        self.name = name
        self.maps = [maps]


class UsersInvalidIndex(IndexDefinition):
    def __init__(self):
        maps = "from u in docs.userwithids select new { a = 5 / u.Age }"
        super().__init__()
        self.name = "InvalidIndex"
        self.maps = [maps]


class TestIndexOperation(TestBase):
    def setUp(self):
        super(TestIndexOperation, self).setUp()

    def test_can_delete_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        names = self.store.maintenance.send(GetIndexNamesOperation(0, 10))
        self.assertEqual(1, len(names))
        self.assertIn("UsersByName", names)

        self.store.maintenance.send(DeleteIndexOperation("UsersByName"))
        names = self.store.maintenance.send(GetIndexNamesOperation(0, 10))

        self.assertEqual(0, len(names))

    def test_can_disable_and_enable_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(DisableIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        index_status = indexing_status.indexes[0]
        self.assertEqual(IndexRunningStatus.DISABLED, index_status.status)

        self.store.maintenance.send(EnableIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        self.assertEqual(IndexRunningStatus.RUNNING, indexing_status.status)

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        index_status = indexing_status.indexes[0]
        self.assertEqual(IndexRunningStatus.RUNNING, index_status.status)

    def test_can_get_index_statistics(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        with self.store.open_session() as session:
            user = UserWithId(None, 0)
            session.store(user)
            session.save_changes()
        self.wait_for_indexing(self.store, self.store.database)
        stats = self.store.maintenance.send(GetIndexStatisticsOperation(index.name))
        self.assertEqual(1, len(stats))

    def test_can_list_errors(self):
        index = UsersInvalidIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        with self.store.open_session() as session:
            user = UserWithId(None, 0)
            session.store(user)
            session.save_changes()
        self.wait_for_indexing(self.store, self.store.database)
        index_errors = self.store.maintenance.send(GetIndexErrorsOperation())
        per_index_errors = self.store.maintenance.send(GetIndexErrorsOperation(index.name))
        self.assertEqual(1, len(index_errors))
        self.assertEqual(1, len(per_index_errors[0].errors))
        self.assertEqual(1, len(per_index_errors[0].errors))

    def test_can_set_index_lock_mode(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(SetIndexesLockOperation(IndexLockMode.LOCKED_ERROR, index.name))
        new_index = self.store.maintenance.send(GetIndexOperation(index.name))
        self.assertEqual(IndexLockMode.LOCKED_ERROR, new_index.lock_mode)

    def test_can_set_index_priority(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(SetIndexesPriorityOperation(IndexPriority.HIGH, index.name))
        new_index = self.store.maintenance.send(GetIndexOperation(index.name))
        self.assertEqual(IndexPriority.HIGH, new_index.priority)

    def test_can_stop_start_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(StopIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, indexing_status.status)
        self.assertEqual(IndexRunningStatus.PAUSED, indexing_status.indexes[0].status)

        self.store.maintenance.send(StartIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, indexing_status.status)
        self.assertEqual(IndexRunningStatus.RUNNING, indexing_status.indexes[0].status)

    def test_can_stop_start_indexing(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(StopIndexingOperation())

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.PAUSED, indexing_status.status)

        self.store.maintenance.send(StartIndexingOperation())

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(IndexRunningStatus.RUNNING, indexing_status.status)

    def test_get_can_indexes(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.assertEqual(1, len(self.store.maintenance.send(GetIndexesOperation(0, 10))))

    def test_get_can_indexes_stats(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.assertEqual(1, len(self.store.maintenance.send(GetIndexesStatisticsOperation())))

    def test_get_terms(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        with self.store.open_session() as session:
            user = UserWithId("Marcin")
            session.store(user)
            session.save_changes()

        self.wait_for_indexing(self.store, self.store.database)

        terms = self.store.maintenance.send(GetTermsOperation(index.name, "name", None))
        self.assertEqual(1, len(terms))
        self.assertIn("marcin", terms)

    def test_has_index_changed(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.assertFalse(self.store.maintenance.send(IndexHasChangedOperation(index)))
        index.maps = ("from users",)
        self.assertTrue(self.store.maintenance.send(IndexHasChangedOperation(index)))
