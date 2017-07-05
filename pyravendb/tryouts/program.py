from pyravendb.store.document_store import DocumentStore
from pyravendb.connection.cluster_requests_executor import ClusterRequestExecutor
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.requests_helpers import *
from pyravendb.d_commands.raven_commands import GetClusterTopologyCommand, GetTopologyCommand
import sys
import json


class Dog(object):
    def __init__(self, Name, Id=None):
        self.Id = Id
        self.Name = Name


class Child(object):
    def __init__(self, Name, Id=None):
        self.Id = Id
        self.Name = Name


class Node(object):
    def __init__(self, changed):
        self.changed = changed


class AA():
    def __init__(self):
        self.prints = "AA"

    def create(self, front, back):
        print(front + self.prints + back)


class BB(AA):
    def __init__(self):
        self.prints = "BB"

    def create(self, front):
        print(front + self.prints)


if __name__ == "__main__":
    with DocumentStore(["http://localhost.fiddler:8082", "http://localhost.fiddler:8080"], "NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            dog = Dog("Faz")
            session.store(Child("Idan"))
            session.store(dog)
            session.save_changes()
            key = session.advanced.get_document_id(dog)

        with store.open_session() as session:
            child = session.load("children/1-A", object_type=Child)
            child.Name = "Haim"
            session.save_changes()

        with store.open_session() as session:
            session.delete(key)
            session.save_changes()
