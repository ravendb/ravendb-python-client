from threading import Event
from ravendb.documents.store.definition import DocumentStore
from ravendb.documents.subscriptions.options import SubscriptionWorkerOptions
from ravendb.exceptions.exceptions import AllTopologyNodesDownException
from ravendb.infrastructure.entities import User
from ravendb.serverwide.operations.common import GetDatabaseRecordOperation
from ravendb.tests.test_base import TestBase
from ravendb.http.topology import UpdateTopologyParameters
from ravendb.http.server_node import ServerNode


class TestRDBC948(TestBase):
    def setUp(self):
        super().setUp()

    def test_failover_with_invalid_dns_in_urls(self):
        # One invalid DNS hostname and one valid server URL (from the embedded test server)
        invalid_host = "http://thisnamedoesnotexist:8080"
        valid_url = self.store.urls[0]

        with DocumentStore(urls=[invalid_host, valid_url], database=self.store.database) as store2:
            store2.conventions.disable_topology_updates = False
            store2.initialize()

            # Should succeed by failing over to the valid URL
            with store2.open_session() as session:
                session.store({"Name": "John"}, "users/1")
                session.save_changes()

            # Verify we can read it back (continues using the healthy node)
            with store2.open_session() as session:
                doc = session.load("users/1")
                self.assertIsNotNone(doc)
                self.assertEqual(doc.get("Name"), "John")

    def test_all_nodes_down_throws(self):
        # Two unreachable endpoints: invalid DNS and a closed localhost port
        urls = [
            "http://thisnamedoesnotexist:8080",
            "http://127.0.0.1:1234",
        ]

        with DocumentStore(urls=urls, database=self.store.database) as store2:
            store2.conventions.disable_topology_updates = False
            store2.initialize()

            with self.assertRaises(AllTopologyNodesDownException):
                with store2.open_session() as session:
                    session.load("users/does-not-matter")

    def test_maintenance_operation_failover_with_invalid_dns(self):
        invalid_host = "http://thisnamedoesnotexist:8080"
        valid_url = self.store.urls[0]
        database = self.store.database

        with DocumentStore(urls=[invalid_host, valid_url], database=database) as store2:
            store2.conventions.disable_topology_updates = False
            store2.initialize()

            # Perform maintenance call, should succeed by failing over
            record = store2.maintenance.server.send(GetDatabaseRecordOperation(database))
            self.assertIsNotNone(record)

    def test_request_executor_failover_with_invalid_dns(self):
        invalid_host = "http://thisnamedoesnotexist:8080"
        valid_url = self.store.urls[0]

        with DocumentStore(urls=[invalid_host, valid_url], database=self.store.database) as store2:
            store2.conventions.disable_topology_updates = False
            store2.initialize()

            # Explicitly refresh topology like C# tests via UpdateTopologyAsync.
            # This avoids racing the background first-topology-update and ensures the selector is initialized.
            req_ex = store2.get_request_executor()
            params = UpdateTopologyParameters(ServerNode(valid_url, store2.database))
            params.timeout_in_ms = 5
            params.debug_tag = "test-init"
            req_ex.update_topology_async(params).result()
            # Now URL should reflect the healthy node
            self.assertIsNotNone(req_ex.url, "request executor URL did not initialize")
            self.assertTrue(req_ex.url.startswith(valid_url), f"unexpected URL: {req_ex.url}")

            # And simple operations should succeed
            with store2.open_session() as session:
                session.store({"Name": "Jane"}, "users/2")
                session.save_changes()

    def test_subscription_failover_with_invalid_dns(self):
        invalid_host = "http://thisnamedoesnotexist:8080"
        valid_url = self.store.urls[0]

        with DocumentStore(urls=[invalid_host, valid_url], database=self.store.database) as store2:
            store2.conventions.disable_topology_updates = False
            store2.initialize()

            # Create a subscription and ensure worker connects and receives items
            sub_id = store2.subscriptions.create_for_class(User)
            with store2.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(sub_id), User) as worker:
                got_item = Event()

                def _run(batch):
                    for item in batch.items:
                        if item.result is not None:
                            got_item.set()

                worker.run(_run)

                # Add a document so the subscription has something to send
                with store2.open_session() as session:
                    session.store(User(name="SubUser"))
                    session.save_changes()

                self.assertTrue(got_item.wait(10))
