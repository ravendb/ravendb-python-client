from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation
from datetime import datetime, timedelta, timezone


class User:
    def __init__(self, name):
        self.name = name


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost:8080"], database="NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            session.store(User("idan"), key="users/1-A")
            session.save_changes()

        # Add time_series to user/1-A document
        time_series_operation = TimeSeriesOperation(name="Heartrate")
        time_series_operation.append(datetime.now(), 73, tag="heart/rates")
        time_series_operation.append(datetime.now() + timedelta(minutes=5), 78, tag="heart/rates")
        time_series_operation.append(datetime(2019, 4, 23) + timedelta(minutes=5), 789, tag="heart/rates")

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="users/1-A", operation=time_series_operation)
        store.operations.send(time_series_batch_operation)

        # Fetch all time_series from the document
        tsr1 = TimeSeriesRange("Heartrate", datetime.now() - timedelta(days=2), datetime.now() + timedelta(days=2))
        tsr = TimeSeriesRange("Heartrate")

        # Fetch without using ranges
        time_series = store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=tsr))
        entries = time_series['Values']["Heartrate"][0]['Entries']
        print(entries, len(entries))

        # Fetch using ranges
        time_series1 = store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=tsr1))
        entries = time_series1['Values']["Heartrate"][0]['Entries']
        print(entries, len(entries))

        # Remove all time_series from the document
        time_series_operation_remove = TimeSeriesOperation(name="Heartrate")
        time_series_operation_remove.remove()

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="users/1-A",
                                                               operation=time_series_operation_remove)
        store.operations.send(time_series_batch_operation)

        time_series = store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=tsr))
        entries = time_series['Values']["Heartrate"][0]['Entries']
        print(entries, len(entries))
