from pyravendb.raven_operations.maintenance_operations import (
    PutIndexesOperation,
    GetIndexNamesOperation,
    DeleteIndexOperation,
    GetIndexingStatusOperation,
    GetIndexStatisticsOperation,
    GetIndexErrorsOperation,
    DisableIndexOperation,
    EnableIndexOperation,
    SetIndexesLockOperation,
    GetIndexOperation,
    SetIndexesPriorityOperation,
    StopIndexOperation,
    StartIndexOperation,
    StopIndexingOperation,
    StartIndexingOperation,
    GetIndexesOperation,
    GetIndexesStatisticsOperation,
    GetTermsOperation,
    IndexHasChangedOperation,
)
from pyravendb.data.indexes import IndexDefinition, IndexingStatus, IndexLockMode, IndexPriority

from pyravendb.tests.test_base import TestBase, UserWithId


class UsersIndex(IndexDefinition):
    def __init__(self, name="UsersByName"):
        maps = "from user in docs.userwithids select new { user.name }"
        super(UsersIndex, self).__init__(name, maps)


class UsersInvalidIndex(IndexDefinition):
    def __init__(self):
        maps = "from u in docs.userwithids select new { a = 5 / u.Age }"
        super(UsersInvalidIndex, self).__init__("InvalidIndex", maps)


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
        index_status = indexing_status["Indexes"][1]
        self.assertEqual(str(IndexingStatus.disabled), index_status["Status"])

        self.store.maintenance.send(EnableIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        self.assertEqual(str(IndexingStatus.running), indexing_status["Status"])

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())
        index_status = indexing_status["Indexes"][1]
        self.assertEqual(str(IndexingStatus.running), index_status["Status"])

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
        per_index_errors = self.store.maintenance.send(GetIndexErrorsOperation([index.name]))
        self.assertEqual(2, len(index_errors))
        self.assertEqual(1, len(per_index_errors[0].errors))
        self.assertEqual(1, len(per_index_errors[0].errors))

    def test_can_set_index_lock_mode(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(SetIndexesLockOperation(IndexLockMode.locked_error, index.name))
        new_index = self.store.maintenance.send(GetIndexOperation(index.name))
        self.assertEqual(str(IndexLockMode.locked_error), new_index.lock_mode)

    def test_can_set_index_priority(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(SetIndexesPriorityOperation(IndexPriority.high, index.name))
        new_index = self.store.maintenance.send(GetIndexOperation(index.name))
        self.assertEqual(str(IndexPriority.high), new_index.priority)

    def test_can_stop_start_index(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(StopIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(str(IndexingStatus.running), indexing_status["Status"])
        self.assertEqual(str(IndexingStatus.paused), indexing_status["Indexes"][1]["Status"])

        self.store.maintenance.send(StartIndexOperation(index.name))

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(str(IndexingStatus.running), indexing_status["Status"])
        self.assertEqual(str(IndexingStatus.running), indexing_status["Indexes"][0]["Status"])

    def test_can_stop_start_indexing(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.store.maintenance.send(StopIndexingOperation())

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(str(IndexingStatus.paused), indexing_status["Status"])

        self.store.maintenance.send(StartIndexingOperation())

        indexing_status = self.store.maintenance.send(GetIndexingStatusOperation())

        self.assertEqual(str(IndexingStatus.running), indexing_status["Status"])

    def test_get_can_indexes(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.assertEqual(2, len(self.store.maintenance.send(GetIndexesOperation(0, 10))))

    def test_get_can_indexes_stats(self):
        index = UsersIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        self.assertEqual(2, len(self.store.maintenance.send(GetIndexesStatisticsOperation())))

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
