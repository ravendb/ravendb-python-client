from pyravendb.store.document_store import DocumentStore
from pyravendb.commands.raven_commands import GetTcpInfoCommand
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation
from pyravendb.raven_operations.admin_operations import PutIndexesOperation, GetStatisticsOperation
from pyravendb.subscriptions.data import SubscriptionCreationOptions
from pyravendb.data.indexes import IndexDefinition
from requests.exceptions import RequestException
from pyravendb.tools.utils import Utils
from pyravendb.tools.utils import _DynamicStructure
from xxhash import xxh64
from enum import Enum
import timeit
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
        return f"The dog name is {self.name} and his brand is {self.brand}"


def test(batch):
    for b in batch.items:
        print(b.result)


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost.fiddler:8080"], database="python_2") as store:
        create_database_operation = CreateDatabaseOperation(database_name="python_subscription")
        try:
            store.admin.server.send(create_database_operation)
        except Exception as e:
            print(e)
        store.initialize()
        with store.open_session() as session:
            for i in range(0, 100):
                dog = Dog("fazi" + str(i), "chi" + str(i + 2))
                user = User("Idan" + str(i), i, dog)
                session.store(dog)
                session.store(user)
            session.save_changes()

        index_map = "from doc in docs.Users select new" \
                    "{name = doc.name," \
                    "age = doc.age," \
                    "dog = doc.dog}"
        store.admin.send(PutIndexesOperation(IndexDefinition("AllUsers", index_map=index_map)))
        subscription_query = store.subscription.create("from Dogs where name = 'fazi1'")

        option = SubscriptionConnectionOptions("10", time_to_wait_before_connection_retry=timedelta(seconds=10))
        subscription = store.subscription.open(option, store.database, object_type=Dog)
        t = subscription.run(test)
        t.join()

        subscription.close()
        subscriptions = store.subscription.get_subscriptions(0, 3)
        for sub in subscriptions:
            print(sub)
