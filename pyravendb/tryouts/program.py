from pyravendb.store.document_store import DocumentStore
from pyravendb.changes.observers import ActionObserver
import time


class User:
    def __init__(self, name, age=0, dog=None):
        self.name = name
        self.dog = dog
        self.age = age


class Dog:
    def __init__(self, name, brand):
        self.name = name
        self.brand = brand

    def __str__(self):
        return "The dog name is " + self.name + " and his brand is " + self.brand


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost.fiddler:8080"], database="Northwind") as store:
        store.initialize()
        # with store.open_session() as session:
        #     for i in range(0, 10000):
        #         session.store(User("Idan", i))
        #     session.save_changes()

        with store.open_session() as session:
            query = session.query(object_type=User, index_name="UserByName")
            count = 0
            results = session.advanced.stream(query)
            for result in results:
                # do something with this
                user = result.get("document", None)
                count += 1
