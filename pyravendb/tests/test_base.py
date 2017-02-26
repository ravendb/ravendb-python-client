from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.server_node import ServerNode
from pyravendb.data.indexes import IndexDefinition
import unittest
import sys
import os

sys.path.append(os.path.abspath(__file__ + "/../../"))

from pyravendb.data.database import DatabaseDocument
from pyravendb.d_commands.raven_commands import CreateDatabaseCommand, DeleteDatabaseCommand, PutIndexesCommand


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.default_url = "http://localhost.fiddler:8080"
        cls.default_database = "NorthWindTest"
        server_node = ServerNode(cls.default_url, cls.default_database)
        cls.requests_executor = RequestsExecutor(server_node)
        cls.requests_executor.execute(
            CreateDatabaseCommand(DatabaseDocument(cls.default_database, {"Raven/DataDir": "test"})))

        cls.index_map = ("from doc in docs "
                         "select new{"
                         "Tag = doc[\"@metadata\"][\"Raven-Entity-Name\"],"
                         "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],"
                         "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks}"
                         )
        cls.index = IndexDefinition("Testing", cls.index_map)
        # cls.requests_executor.execute(PutIndexesCommand(cls.index))

    @classmethod
    def tearDownClass(cls):
        cls.requests_executor.execute(DeleteDatabaseCommand("NorthWindTest", True))
