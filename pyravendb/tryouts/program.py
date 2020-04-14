from datetime import datetime
from datetime import timedelta

from pyravendb.raven_operations.operations import GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, \
    TimeSeriesOperation, TimeSeriesBatch
from pyravendb.store.document_store import DocumentStore

if __name__ == "__main__":
    pass

with DocumentStore(urls=["http://localhost:8080"], database="demo") as store:
    store.initialize()

    batch = TimeSeriesBatch()
    batch.append("employees/7-A", "Heartrate", datetime.now(), 54, None)

    a = store.operations.send(TimeSeriesBatchOperation(batch))
    print(repr(a))

    r = TimeSeriesRange("Heartrate", datetime.now() - timedelta(days=2), datetime.now() + timedelta(days=2))
    result = store.operations.send(GetTimeSeriesOperation("employees/7-A", r))
    print (repr(result))