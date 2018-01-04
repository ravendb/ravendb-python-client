from pyravendb.data.indexes import IndexDefinition, IndexFieldOptions, FieldIndexing
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation
from pyravendb.custom_exceptions import exceptions
from pyravendb.store.document_store import DocumentStore
from pyravendb.subscriptions.data import *
import threading


class User:
    def __init__(self, name=None, age=0, dog=None):
        self.name = name
        self.dog = dog
        self.age = age


class Dog:
    def __init__(self, name, brand):
        self.name = name
        self.brand = brand

    def __str__(self):
        return "The dog name is " + self.name + " and his brand is " + self.brand


def test(batch):
    for b in batch.items:
        print(b.result)


class Time(object):
    def __init__(self, td, dt):
        self.td = td
        self.dt = dt


class UsersByName:
    def __init__(self):
        self.index_map = """from doc in docs.Users
                      select new{name = doc.name}"""

        self.index_definition = IndexDefinition(name=UsersByName.__name__, maps=self.index_map,
                                                fields={"name": IndexFieldOptions(indexing=FieldIndexing.search)})

    def execute(self, document_store):
        document_store.maintenance.send(PutIndexesOperation(self.index_definition))


class Test(Exception):
    pass


event = threading.Event()


def process_documents(self, batch, items_count=1):
    items_count += len(batch.items)
    for b in batch.items:
        self.results.append(b.result)
    if self.items_count == self.expected_items_count:
        event.set()


class Test2(Test):
    def __init__(self, number=0, message=""):
        super(Test2, self).__init__(message)
        self.number = number


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost:8081"], database="NorthWind") as store:
        store.initialize()

        connection_options = SubscriptionWorkerOptions("testing")
        with store.subscriptions.get_subscription_worker(connection_options) as subscription:
            subscription.run(process_documents)
            event.wait()
