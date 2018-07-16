from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation, DeleteDatabaseOperation
import hashlib
from shutil import rmtree
import os
import unittest

TOPOLOGY_FILES_DIR = os.path.join(os.getcwd(), "topology_files")
DATABASE = "SystemTest"


class Author:
    def __init__(self, name):
        self.name = name


class TestSystemTopologyCreation(TestBase):
    def tearDown(self):
        self.store.maintenance.server.send(
            DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
        super(TestSystemTopologyCreation, self).tearDown()
        TestBase.delete_all_topology_files()
        if os.path.exists(TOPOLOGY_FILES_DIR):
            try:
                rmtree(TOPOLOGY_FILES_DIR, ignore_errors=True)
            except OSError as ex:
                print(ex)

    def test_topology_creation(self):
        created = False
        while not created:
            try:
                self.store.maintenance.server.send(CreateDatabaseOperation(database_name=DATABASE))
                created = True
            except Exception as e:
                if "already exists!" in str(e):
                    self.store.maintenance.server.send(
                        DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
                    continue
                raise
        TestBase.wait_for_database_topology(self.store, DATABASE)

        with DocumentStore(urls=self.default_urls, database=DATABASE) as store:
            store.initialize()
            with store.open_session() as session:
                session.store(Author("Idan"))
                session.save_changes()
        topology_hash = hashlib.md5(
            "{0}{1}".format(self.default_urls[0], DATABASE).encode(
                'utf-8')).hexdigest()

        cluster_topology_hash = hashlib.md5(
            "{0}".format(self.default_urls[0]).encode(
                'utf-8')).hexdigest()

        self.assertTrue(os.path.exists(os.path.join(TOPOLOGY_FILES_DIR, topology_hash + ".raven-topology")))
        self.assertTrue(
            os.path.exists(os.path.join(TOPOLOGY_FILES_DIR, cluster_topology_hash + ".raven-cluster-topology")))


if __name__ == "__main__":
    unittest.main()
