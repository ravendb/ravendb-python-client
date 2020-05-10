import unittest
import sys
import os

sys.path.append(os.path.abspath(__file__ + "/../../"))

from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.server_operations import *
from pyravendb.raven_operations.maintenance_operations import IndexDefinition, PutIndexesOperation


class User(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age


class Dog(object):
    def __init__(self, name, owner):
        self.name = name
        self.owner = owner


class Patch(object):
    def __init__(self, patched):
        self.patched = patched


class TestBase(unittest.TestCase):
    @staticmethod
    def delete_all_topology_files():
        import os
        file_list = [f for f in os.listdir(".") if f.endswith("topology")]
        for f in file_list:
            os.remove(f)

    @staticmethod
    def wait_for_database_topology(store, database_name, replication_factor=1):
        topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name))
        while topology is not None and len(topology["Members"]) < replication_factor:
            topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name))
        return topology

    def setConvention(self, conventions):
        self.conventions = conventions

    def setUp(self):
        conventions = getattr(self, "conventions", None)
        self.default_urls = ["http://127.0.0.1:8080"]
        self.default_database = "NorthWindTest"
        self.store = DocumentStore(urls=self.default_urls, database=self.default_database)
        if conventions:
            self.store.conventions = conventions
        self.store.initialize()
        created = False
        while not created:
            try:
                self.store.maintenance.server.send(CreateDatabaseOperation(database_name=self.default_database))
                created = True
            except Exception as e:
                if "already exists!" in str(e):
                    self.store.maintenance.server.send(
                        DeleteDatabaseOperation(database_name=self.default_database, hard_delete=True))
                    continue
                raise
        TestBase.wait_for_database_topology(self.store, self.default_database)

        self.index_map = "from doc in docs select new{Tag = doc[\"@metadata\"][\"@collection\"]}"
        self.store.maintenance.send(PutIndexesOperation(IndexDefinition("AllDocuments", maps=self.index_map)))

    def tearDown(self):
        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name="NorthWindTest", hard_delete=True))
        self.store.close()
