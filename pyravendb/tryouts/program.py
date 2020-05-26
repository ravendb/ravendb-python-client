from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from pyravendb.raven_operations.counters_operations import *
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation, IndexDefinition, \
    PutConnectionStringOperation, ConnectionString, UpdateExternalReplicationOperation, ExternalReplication, \
    PutPullReplicationAsHubOperation, PullReplicationDefinition, UpdatePullReplicationAsSinkOperation, \
    PullReplicationAsSink
from datetime import datetime, timedelta, timezone
from time import sleep


class User:
    def __init__(self, name, address):
        self.Id = None
        self.name = name
        self.address = address


class Address:
    def __init__(self, street):
        self.street = street


def get_user(key, value):
    if key == "address":
        return Address(**value)
    return value


if __name__ == "__main__":
    with DocumentStore(urls=["http://live-test.ravendb.net"], database="demo") as store:
        store.initialize()

        # store.maintenance.send(PutConnectionStringOperation(ConnectionString.raven(
        #     "con_str", "Northwind", ["http://live-test.ravendb.net"]
        # )))
        #

        store.maintenance.send(UpdatePullReplicationAsSinkOperation(PullReplicationAsSink(
            "foo", "con_str", None, None
        )))

        # store.maintenance.server.send(CreateDatabaseOperation("NorthWindccxx"))
        # store.maintenance.server.send(DeleteDatabaseOperation("NorthWindccxx", hard_delete=True))
        # with store.open_session() as session:
        #     session.store(User("idan", address="mm"), "users/1")
        #     session.save_changes()
        #
        # with store.open_session() as session:
        #     user = session.load('users/1', object_type=User)

        # with store.open_session() as session:
        #     counters = session.counters_for('users/2')
        #     counters.increment("likes", delta=20)
        #     counters.increment("love", delta=10)
        #     session.save_changes()
        #
        # with store.open_session() as session:
        #     counters = session.counters_for('users/2')
        #     counters.delete("likes")
        #     session.save_changes()
        #
        # with store.open_session() as session:
        #     document_counter = session.counters_for("users/2")
        #     counters = document_counter.get_all()
        #     like = document_counter.get("likes0")
        #     print(like)
        #
        # names = []
        # for i in range(1033):
        #     names.append(f"likes{i}")
        # names.append(None)
        # xc = store.operations.send(GetCountersOperation("users/2", counters=names))
        # print(xc)
        # print(len(xc["Counters"]))

        # d = DocumentCountersOperation(document_id='users/2')
        # d.add_operations(CounterOperation("dooms", counter_operation_type=CounterOperationType.increment, delta=4))
        # d.add_operations(CounterOperation("dooms", counter_operation_type=CounterOperationType.delete))
        #
        # counter_batch = CounterBatch([d])
        # v = store.operations.send(CounterBatchOperation(counter_batch))
        # print(v)
