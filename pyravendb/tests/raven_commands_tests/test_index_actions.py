import unittest
from pyravendb.data.indexes import IndexDefinition, IndexSourceType
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation, GetIndexOperation
from pyravendb.commands.raven_commands import DeleteIndexCommand
from pyravendb.tests.test_base import TestBase
from datetime import datetime


class User:
    def __init__(self, name):
        self.name = name


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

    def test_timeseries_index_creation(self):
        with self.store.open_session() as session:
            user = User("Idan")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.time_series_for("users/1", "HeartRate").append(datetime.now(), values=98)
            session.save_changes()

        map_ = (
                "timeseries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {" +
                "   beat = entry.Values[0], " +
                "   date = entry.Timestamp.Date, " +
                "   user = ts.DocumentId " +
                "});")
        index_definition = IndexDefinition(name="test_index", maps=map_)
        self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual(index_definition.source_type, IndexSourceType.time_series)

    def test_counters_index_creation(self):
        with self.store.open_session() as session:
            user = User("Idan")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("Shares", 1)
            session.save_changes()

        map_ = (
                "from counter in counters.Users.Shares " +
                "select new { " +
                "   delta = counter.Value, " +
                "   name = counter.Name," +
                "   user = counter.DocumentId " +
                "}")
        index_definition = IndexDefinition(name="counters_index", maps=map_)
        self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual(index_definition.source_type, IndexSourceType.counters)


if __name__ == "__main__":
    unittest.main()
