"""
Unit tests for the 7.2.3 topology-command response validation.

When the URL doesn't point at a RavenDB server, the JSON parse may succeed
but the resulting object won't have the expected fields. The client should
raise a clear "may indicate that the URL does not point to a RavenDB server"
error.
"""

import unittest

from ravendb.serverwide.commands import GetClusterTopologyCommand, GetDatabaseTopologyCommand


class TestGetDatabaseTopologyCommandValidation(unittest.TestCase):
    def test_response_without_nodes_raises(self):
        cmd = GetDatabaseTopologyCommand()
        with self.assertRaises(RuntimeError) as ctx:
            cmd.set_response('{"NotATopology": true}', from_cache=False)
        self.assertIn("does not point to a RavenDB server", str(ctx.exception))

    def test_malformed_json_raises_friendly(self):
        cmd = GetDatabaseTopologyCommand()
        with self.assertRaises(RuntimeError) as ctx:
            cmd.set_response("<html>not json</html>", from_cache=False)
        self.assertIn("does not point to a RavenDB server", str(ctx.exception))

    def test_none_response_is_silent(self):
        cmd = GetDatabaseTopologyCommand()
        # None means "no response" — matches existing behavior (no raise).
        cmd.set_response(None, from_cache=False)


class TestGetClusterTopologyCommandValidation(unittest.TestCase):
    def test_response_missing_topology_raises(self):
        cmd = GetClusterTopologyCommand()
        with self.assertRaises(RuntimeError) as ctx:
            cmd.set_response('{"Leader": "A", "NodeTag": "A"}', from_cache=False)
        self.assertIn("does not point to a RavenDB server", str(ctx.exception))

    def test_malformed_json_raises_friendly(self):
        cmd = GetClusterTopologyCommand()
        with self.assertRaises(RuntimeError) as ctx:
            cmd.set_response("<html>not json</html>", from_cache=False)
        self.assertIn("does not point to a RavenDB server", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
