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
    with DocumentStore(urls=["http://localhost:8084"], database="Northwind") as store:
        store.initialize()
        all_documents = []
        for_document = []

        changes = store.changes()
        all_observer = changes.for_all_documents()
        all_observer.subscribe(all_documents.append)
        all_observer.ensure_subscribe_now()

        observer = changes.for_document("users/1-A")
        observer.subscribe(ActionObserver(on_next=for_document.append))
        observer.ensure_subscribe_now()

        observer = changes.for_document("users/2-A")
        observer.subscribe(for_document.append)
        observer.ensure_subscribe_now()

        with store.open_session() as session:
            session.store(User(name="Idan"), key="users/1-A")
            session.store(User(name="Shalom"), key="users/2-A")
            session.store(User(name="Ilay"), key="users/3-A")
            session.save_changes()

        time.sleep(1)
        print(all_documents)
        print(for_document)
