from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation
from datetime import datetime, timedelta, timezone


class User:
    def __init__(self, name):
        self.name = name


if __name__ == "__main__":
    with DocumentStore(urls=["http://127.0.0.1:8080"], database="NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            session.store(User("idan"), key="users/1-A")
            session.save_changes()

        with store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "heartrate")
            tsf.append(datetime.now(), values=10, tag="fizz/ozz")
            tsf.append(datetime.now() - timedelta(hours=10), values=[11, 23, 45], tag="fizz/ozz")
            session.save_changes()

        with store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "heartrate")
            v = tsf.get()
            tsf.remove(datetime.now(), datetime.now() + timedelta(hours=1))
            tsf.remove(datetime.now() - timedelta(days=2), datetime.now())
            session.save_changes()

        with store.open_session() as session:
            c = session.time_series_for("users/1-A", "Heartrate")
            v = c.get()
        print(v)
