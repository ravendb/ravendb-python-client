import unittest

from pyravendb.documents.indexes import IndexDefinition, IndexSourceType
from pyravendb.documents.operations.indexes import PutIndexesOperation, GetIndexOperation, DeleteIndexOperation
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
        index = IndexDefinition()
        index.name = "region"
        index.maps = self.index_map
        self.requests_executor.execute_command(PutIndexesOperation(index).get_command(self.conventions))

    def test_get_index_success(self):
        index = IndexDefinition()
        index.name = "get_index"
        index.maps = self.index_map
        self.requests_executor.execute_command(PutIndexesOperation(index).get_command(self.conventions))
        command = GetIndexOperation("get_index").get_command(self.conventions)
        self.requests_executor.execute_command(command)
        self.assertIsNotNone(command.result)

    def test_get_index_fail(self):
        self.assertIsNone(
            self.requests_executor.execute_command(GetIndexOperation("get_index").get_command(self.conventions))
        )

    def test_delete_index_success(self):
        index = IndexDefinition()
        index.name = "delete"
        index.maps = self.index_map

        self.requests_executor.execute_command(PutIndexesOperation(index).get_command(self.conventions))
        command = DeleteIndexOperation("delete").get_command(self.conventions)
        self.requests_executor.execute_command(command)

        command = GetIndexOperation("delete").get_command(self.conventions)
        self.requests_executor.execute_command(command)
        self.assertIsNone(command.result)

    def test_delete_index_fail(self):
        with self.assertRaises(ValueError):
            command = DeleteIndexOperation(None).get_command(self.conventions)
            self.requests_executor.execute_command(command)

    def test_timeseries_index_creation(self):
        with self.store.open_session() as session:
            user = User("Idan")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.time_series_for("users/1", "HeartRate").append(
                datetime.now(), values=98
            )  # todo: implement time_series_for
            session.save_changes()

        self.map_ = (
            "timeseries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {"
            + "   beat = entry.Values[0], "
            + "   date = entry.Timestamp.Date, "
            + "   user = ts.DocumentId "
            + "});"
        )

        index = IndexDefinition()
        index.name = "test_index"
        index.maps = self.map_

        self.store.maintenance.send(PutIndexesOperation(index))

        self.assertEqual(index.source_type, IndexSourceType.time_series)

    def test_counters_index_creation(self):
        with self.store.open_session() as session:
            user = User("Idan")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("Shares", 1)  # todo: implement counters_for
            session.save_changes()

        map_ = (
            "from counter in counters.Users.Shares "
            + "select new { "
            + "   delta = counter.Value, "
            + "   name = counter.Name,"
            + "   user = counter.DocumentId "
            + "}"
        )
        index_definition = IndexDefinition()
        index_definition.name = "test_index"
        index_definition.maps = map_
        self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual(index_definition.source_type, IndexSourceType.counters)


if __name__ == "__main__":
    unittest.main()
