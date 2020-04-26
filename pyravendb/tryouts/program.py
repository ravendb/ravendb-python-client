from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation, IndexDefinition
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation
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
    with DocumentStore(urls=["http://127.0.0.1:8080"], database="NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            user = User("Idan", Address("Rubin"))
            session.store(user, "users/1")
            session.save_changes()

        with store.open_session() as session:
            user = session.load(user.Id, object_type=User)
            session.time_series_for(user, "Beat").append(datetime.now(), values=98)
            session.save_changes()

        map_ = (
                "timeseries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {" +
                "   Beat = entry.Values[0], " +
                "   Date = entry.Timestamp.Date, " +
                "   User = ts.DocumentId " +
                "});")
        index_definition = IndexDefinition(name="test_index", maps=map_)
        c = index_definition.to_json()
        store.maintenance.send(PutIndexesOperation(index_definition))

        print("Tryouts")
