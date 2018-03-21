from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.operations import *
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
        #     session.advanced.attachment.delete("users/1-A", "photo.jpg")
        #     session.save_changes()

        # with store.open_session() as session:
        #     session.store(User("Idan", 30), "users/1-A")
        #     session.save_changes()

        with open('output.txt', 'rb') as file_stream:
            with store.open_session() as session:
                # user = session.load("users/1-A")
                session.advanced.attachment.store("users/1-A", "my_binary_list", file_stream, content_type="text/plain")
                session.save_changes()

        with store.open_session() as session:
            attachment = session.advanced.attachment.get("users/1-A", "my_binary_list")
            if attachment is not None:
                print("yes")
