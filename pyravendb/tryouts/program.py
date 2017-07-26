from pyravendb.store.document_store import DocumentStore
from pyravendb.connection.cluster_requests_executor import ClusterRequestExecutor
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.requests_helpers import *
from pyravendb.data.operation import AttachmentType
from pyravendb.d_commands.raven_commands import GetClusterTopologyCommand, GetTopologyCommand
import sys
import timeit
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


def return_none_with_not_return(number):
    if number > 30:
        return "Bravo"


class nome:
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c


class noma(nome):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for key, value in kwargs.items():
            print(key + " " + value)


def join():
    j = []
    j.append("1")
    j.append("2")
    j.append("3")
    return "".join(j)


def plus():
    j = "1"
    j += "2"
    j += "3"
    return j


def formats():
    j = "{0}{1}{2}".format(1, 2, 3)
    return j


if __name__ == "__main__":
    x = {"s": "x"}
    for y in x:
        print(y)
    print("hallo {0}".format(x))

    # first_store = DocumentStore(["http://localhost:8080"], "NorthWind")
    # first_store.initialize()
    # second_store = DocumentStore(["http://localhost:8081"], "NorthWind")
    # second_store.initialize()
    #
    # session = first_store.open_session()
    # document_store = session.advanced.document_store
    # session._document_store = second_store
    # session.advanced.session
    #
    # with DocumentStore(["http://localhost:8080"], "NorthWind") as store:
    #     store.initialize()
    #
    #     with store.open_session() as session:
    #         dog = Dog("Faz")
    #         session.store(dog, id=3)
    #         session.store(Child("Idan"))
    #         session.store(dog)
    #         session.save_changes()
    #         key = session.advanced.get_document_id(dog)
    #
    #     with store.open_session() as session:
    #         child = session.load("children/1-A", object_type=Child)
    #         child.Name = "Haimz"
    #         session.save_changes()
    #
    #     with store.open_session() as session:
    #         child = session.load("children/1-A", object_type=Child)
    #         child.Name = "Shalomz"
    #         session.save_changes()
