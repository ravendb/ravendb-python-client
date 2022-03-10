from ravendb.documents.store.definition import DocumentStore
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import DeleteDatabaseOperation, CreateDatabaseOperation
from ravendb.tests.test_base import TestBase
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
        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
        super(TestSystemTopologyCreation, self).tearDown()
        TestBase.delete_all_topology_files()
        if os.path.exists(TOPOLOGY_FILES_DIR):
            try:
                rmtree(TOPOLOGY_FILES_DIR, ignore_errors=True)
            except OSError as ex:
                pass

    @unittest.skip("Topology creation")
    def test_topology_creation(self):
        created = False
        while not created:
            resp = self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(DATABASE)))
            if not resp:
                self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
                continue
            created = True
        TestBase.wait_for_database_topology(self.store, DATABASE)

        with DocumentStore(urls=self.default_urls, database=DATABASE) as store:
            store.initialize()
            with store.open_session() as session:
                session.store(Author("Idan"))
                session.save_changes()
        topology_hash = hashlib.md5("{0}{1}".format(self.default_urls[0], DATABASE).encode("utf-8")).hexdigest()

        cluster_topology_hash = hashlib.md5("{0}".format(self.default_urls[0]).encode("utf-8")).hexdigest()

        self.assertTrue(
            os.path.exists(os.path.join(TOPOLOGY_FILES_DIR, topology_hash + ".raven-topology"))
        )  # todo: fix from here - make sure the right folder and files appear
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    TOPOLOGY_FILES_DIR,
                    cluster_topology_hash + ".raven-cluster-topology",
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
