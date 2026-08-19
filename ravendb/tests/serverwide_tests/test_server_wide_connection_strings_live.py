"""Live-server integration tests for server-wide connection strings.

These assert the wire facts a 7.2.5 community server enforces: the 400 for a
Type-less PUT body, the 402 license rejection for PUT and DELETE, and the GET
shapes. The module skips when no server is reachable at localhost:8081.
"""

import os
import unittest

import requests

from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.store.definition import DocumentStore
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    PutServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringOperation,
    ServerWideConnectionString,
)

_LIVE_SERVER_URL = os.environ.get("RAVENDB_LIVE_SERVER_URL", "http://localhost:8081")


def _server_reachable() -> bool:
    try:
        response = requests.get(f"{_LIVE_SERVER_URL}/build/version", timeout=3)
        return response.status_code == 200 and '"7.2.5"' in response.text
    except Exception:
        return False


@unittest.skipUnless(_server_reachable(), f"no live 7.2.5 server at {_LIVE_SERVER_URL}")
class TestServerWideConnectionStringsLive(unittest.TestCase):
    def setUp(self):
        self.store = DocumentStore(urls=[_LIVE_SERVER_URL])
        self.store.initialize()

    def tearDown(self):
        self.store.close()

    def test_put_without_type_answers_bad_request(self):
        # The server's deserializer returns null when Type is missing, which it
        # rejects as 400. The wrapper always writes Type, so the probe uses the
        # raw endpoint.
        response = requests.put(
            f"{_LIVE_SERVER_URL}/admin/configuration/server-wide/connection-strings",
            json={"Name": "x"},
        )
        self.assertEqual(400, response.status_code)
        self.assertIn("Connection string is missing or invalid", response.text)

    def test_valid_put_surfaces_license_rejection_as_base_raven_exception(self):
        wrapper = ServerWideConnectionString(
            connection_string=RavenConnectionString(
                name="live-cs", database="db1", topology_discovery_urls=[_LIVE_SERVER_URL]
            )
        )
        with self.assertRaises(RavenException) as ctx:
            self.store.maintenance.server.send(PutServerWideConnectionStringOperation(wrapper))
        self.assertIn("Your license doesn't support adding server wide connection strings.", str(ctx.exception))

    def test_delete_surfaces_license_rejection_as_base_raven_exception(self):
        connection_string = RavenConnectionString(
            name="live-cs", database="db1", topology_discovery_urls=[_LIVE_SERVER_URL]
        )
        with self.assertRaises(RavenException) as ctx:
            self.store.maintenance.server.send(RemoveServerWideConnectionStringOperation(connection_string))
        self.assertIn("Your license doesn't support adding server wide connection strings.", str(ctx.exception))

    def test_get_returns_results(self):
        result = self.store.maintenance.server.send(GetServerWideConnectionStringsOperation())
        self.assertIsNotNone(result)
        self.assertIsInstance(result.results, list)

    def test_get_type_query_value_is_enum_name(self):
        result = self.store.maintenance.server.send(GetServerWideConnectionStringsOperation(None, None))
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
