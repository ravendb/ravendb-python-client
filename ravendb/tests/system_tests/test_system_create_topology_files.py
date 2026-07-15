import hashlib
import os
import unittest
from shutil import rmtree

from ravendb.documents.store.definition import DocumentStore
from ravendb.http import topology_local_cache
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import Topology
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import DeleteDatabaseOperation, CreateDatabaseOperation
from ravendb.tests.test_base import TestBase

TOPOLOGY_FILES_DIR = os.path.join(os.getcwd(), "topology_files")
DATABASE = "SystemTest"


class Author:
    def __init__(self, name):
        self.name = name


class TestSystemTopologyCreation(TestBase):
    def _customize_store(self, store: DocumentStore) -> None:
        # opt in to the on-disk topology cache for this test's stores
        store.conventions.topology_cache_location = TOPOLOGY_FILES_DIR

    def tearDown(self):
        try:
            self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
        except Exception:
            pass
        super(TestSystemTopologyCreation, self).tearDown()
        TestBase.delete_all_topology_files()
        if os.path.exists(TOPOLOGY_FILES_DIR):
            rmtree(TOPOLOGY_FILES_DIR, ignore_errors=True)

    def test_topology_creation(self):
        created = False
        while not created:
            resp = self.store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(DATABASE)))
            if not resp:
                self.store.maintenance.server.send(DeleteDatabaseOperation(database_name=DATABASE, hard_delete=True))
                continue
            created = True
        TestBase.wait_for_database_topology(self.store, DATABASE)

        # the embedded server uses a random port, so hash the real server url (not the placeholder default_urls)
        base_url = self.store.urls[0]

        with DocumentStore(urls=self.store.urls, database=DATABASE) as store:
            store.conventions.topology_cache_location = TOPOLOGY_FILES_DIR
            store.initialize()
            with store.open_session() as session:
                session.store(Author("Idan"))
                session.save_changes()

        topology_hash = hashlib.md5("{0}{1}".format(base_url, DATABASE).encode("utf-8")).hexdigest()
        cluster_topology_hash = hashlib.md5("{0}".format(base_url).encode("utf-8")).hexdigest()

        self.assertTrue(os.path.exists(os.path.join(TOPOLOGY_FILES_DIR, topology_hash + ".raven-topology")))
        self.assertTrue(
            os.path.exists(os.path.join(TOPOLOGY_FILES_DIR, cluster_topology_hash + ".raven-cluster-topology"))
        )

    def test_topology_cache_round_trip(self):
        os.makedirs(TOPOLOGY_FILES_DIR, exist_ok=True)
        topology = Topology(3, [ServerNode("http://localhost:9999", "db1", "B", ServerNode.Role.MEMBER)])
        topology_hash = topology_local_cache.server_hash("http://localhost:9999", "db1")
        topology_local_cache.try_save(
            TOPOLOGY_FILES_DIR, topology_hash, topology, topology_local_cache.DATABASE_TOPOLOGY_EXTENSION
        )

        loaded = topology_local_cache.try_load(
            TOPOLOGY_FILES_DIR, topology_hash, topology_local_cache.DATABASE_TOPOLOGY_EXTENSION
        )
        self.assertIsNotNone(loaded)
        self.assertEqual(3, loaded.etag)
        self.assertEqual(1, len(loaded.nodes))
        node = loaded.nodes[0]
        self.assertEqual("http://localhost:9999", node.url)
        self.assertEqual("db1", node.database)
        self.assertEqual("B", node.cluster_tag)
        self.assertEqual(ServerNode.Role.MEMBER, node.server_role)

    def test_topology_is_loaded_from_cache_when_urls_unreachable(self):
        bad_url = "http://127.0.0.1:1"
        database = "CacheSeedTest"
        os.makedirs(TOPOLOGY_FILES_DIR, exist_ok=True)
        cached = Topology(9, [ServerNode(bad_url, database, "A", ServerNode.Role.MEMBER)])
        topology_local_cache.try_save(
            TOPOLOGY_FILES_DIR,
            topology_local_cache.server_hash(bad_url, database),
            cached,
            topology_local_cache.DATABASE_TOPOLOGY_EXTENSION,
        )

        # the server is unreachable, so the first topology update must fall back to the on-disk cache
        with DocumentStore(urls=[bad_url], database=database) as store:
            store.conventions.topology_cache_location = TOPOLOGY_FILES_DIR
            store.initialize()
            request_executor = store.get_request_executor()
            request_executor._first_topology_update_task.result(30)

            nodes = request_executor.topology_nodes
            self.assertEqual(1, len(nodes))
            self.assertEqual("A", nodes[0].cluster_tag)
            self.assertEqual(bad_url, nodes[0].url)
            self.assertEqual(ServerNode.Role.MEMBER, nodes[0].server_role)
            self.assertEqual(9, request_executor.topology_etag)


if __name__ == "__main__":
    unittest.main()
