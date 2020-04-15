from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from datetime import datetime, timedelta

if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost:8080"], database="NorthWind") as store:
        store.initialize()
        # Add time_series to user/1-A document
        time_series_operation = TimeSeriesOperation(name="Heartrate")
        time_series_operation.append(datetime.now(), 73, tag="heart/rates")
        time_series_operation.append(datetime.now() + timedelta(minutes=5), 78, tag="heart/rates")

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="user/1-A", operation=time_series_operation)
        store.operations.send(time_series_batch_operation)

        # Fetch all time_series from the document
        tsr = TimeSeriesRange("Heartrate", datetime.now() - timedelta(days=2), datetime.now() + timedelta(days=2))
        time_series = store.operations.send(GetTimeSeriesOperation("user/1-A", ranges=tsr))
        print(len(time_series['Values']["Heartrate"][0]['Entries']))

        # Remove all time_series from the document
        time_series_operation = TimeSeriesOperation(name="Heartrate")
        time_series_operation.remove(datetime.now() - timedelta(days=2), datetime.now() + timedelta(days=2))

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="user/1-A", operation=time_series_operation)
        store.operations.send(time_series_batch_operation)

        tsr = TimeSeriesRange("Heartrate", datetime.now() - timedelta(days=2), datetime.now() + timedelta(days=2))
        time_series = store.operations.send(GetTimeSeriesOperation("user/1-A", ranges=tsr))
        print(len(time_series['Values']["Heartrate"][0]['Entries']))