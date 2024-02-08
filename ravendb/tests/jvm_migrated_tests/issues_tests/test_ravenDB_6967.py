from datetime import timedelta

from ravendb import IndexDefinition
from ravendb.documents.operations.indexes import (
    DeleteIndexErrorsOperation,
    PutIndexesOperation,
    StopIndexingOperation,
    GetIndexErrorsOperation,
)
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestRavenDB6967(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_delete_index_errors(self):
        RavenTestHelper.assert_no_index_errors(self.store)

        self.store.maintenance.send(DeleteIndexErrorsOperation())

        with self.assertRaises(RuntimeError):
            self.store.maintenance.send(DeleteIndexErrorsOperation(["DoesNotExist"]))

        index1 = IndexDefinition(name="Index1")
        index1.maps = ["from doc in docs let x = 0 select new { Total = 3/x };"]

        self.store.maintenance.send(PutIndexesOperation(index1))

        index2 = IndexDefinition(name="Index2")
        index2.maps = ["from doc in docs let x = 0 select new { Total = 4/x };"]

        self.store.maintenance.send(PutIndexesOperation(index2))

        index3 = IndexDefinition(name="Index3")
        index3.maps = ["from doc in docs let x = 0 select new { Total = 5/x };"]

        self.store.maintenance.send(PutIndexesOperation(index3))

        self.wait_for_indexing(self.store)

        RavenTestHelper.assert_no_index_errors(self.store)

        self.store.maintenance.send(DeleteIndexErrorsOperation())

        self.store.maintenance.send(DeleteIndexErrorsOperation(["Index1", "Index2", "Index3"]))

        with self.assertRaises(RuntimeError):
            self.store.maintenance.send(DeleteIndexErrorsOperation(["Index1", "DoesNotExist"]))

        with self.store.open_session() as session:
            session.store(Company())
            session.store(Company())
            session.store(Company())

            session.save_changes()

        self.wait_for_indexing_errors(self.store, timedelta(minutes=1), "Index1")
        self.wait_for_indexing_errors(self.store, timedelta(minutes=1), "Index2")
        self.wait_for_indexing_errors(self.store, timedelta(minutes=1), "Index3")

        self.store.maintenance.send(StopIndexingOperation())

        index_errors1 = self.store.maintenance.send(GetIndexErrorsOperation("Index1"))
        index_errors2 = self.store.maintenance.send(GetIndexErrorsOperation("Index2"))
        index_errors3 = self.store.maintenance.send(GetIndexErrorsOperation("Index3"))

        self.assertGreater(sum([len(x.errors) for x in index_errors1]), 0)
        self.assertGreater(sum([len(x.errors) for x in index_errors2]), 0)
        self.assertGreater(sum([len(x.errors) for x in index_errors3]), 0)

        self.store.maintenance.send(DeleteIndexErrorsOperation(["Index2"]))

        index_errors1 = self.store.maintenance.send(GetIndexErrorsOperation("Index1"))
        index_errors2 = self.store.maintenance.send(GetIndexErrorsOperation("Index2"))
        index_errors3 = self.store.maintenance.send(GetIndexErrorsOperation("Index3"))

        self.assertGreater(sum([len(x.errors) for x in index_errors1]), 0)
        self.assertEqual(sum([len(x.errors) for x in index_errors2]), 0)
        self.assertGreater(sum([len(x.errors) for x in index_errors3]), 0)

        self.store.maintenance.send(DeleteIndexErrorsOperation())

        index_errors1 = self.store.maintenance.send(GetIndexErrorsOperation("Index1"))
        index_errors2 = self.store.maintenance.send(GetIndexErrorsOperation("Index2"))
        index_errors3 = self.store.maintenance.send(GetIndexErrorsOperation("Index3"))

        self.assertEqual(sum([len(x.errors) for x in index_errors1]), 0)
        self.assertEqual(sum([len(x.errors) for x in index_errors2]), 0)
        self.assertEqual(sum([len(x.errors) for x in index_errors3]), 0)

        RavenTestHelper.assert_no_index_errors(self.store)
