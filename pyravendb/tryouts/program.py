from pyravendb.store.document_store import DocumentStore
from pyravendb.store import document_store
from pyravendb.commands.raven_commands import GetTcpInfoCommand
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation, DeleteDatabaseOperation
from pyravendb.raven_operations.admin_operations import PutIndexesOperation, GetStatisticsOperation
from pyravendb.subscriptions.data import SubscriptionCreationOptions
from pyravendb.data.indexes import IndexDefinition, IndexFieldOptions, FieldIndexing
from requests.exceptions import RequestException
from pyravendb.tools.utils import Utils
from pyravendb.tools.utils import _DynamicStructure
from xxhash import xxh64
from enum import Enum
import timeit
import time
from datetime import timedelta
from threading import Thread
from urllib.parse import urlsplit, urlparse
from pyravendb.custom_exceptions.exceptions import *
from pyravendb.subscriptions.subscription import Subscription
from  pyravendb.subscriptions.data import SubscriptionConnectionOptions


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
        return "The dog name is" + self.name + " and his brand is" + self.brand


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
        document_store.admin.send(PutIndexesOperation(self.index_definition))


if __name__ == "__main__":
    store = document_store.DocumentStore(urls=["http://localhost:8080", "http://localhost:8084"], database="PyRavenDB")
    store.initialize()
    with store.open_session() as session:
        foo = session.load("foos/1")
