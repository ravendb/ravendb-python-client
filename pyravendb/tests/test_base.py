from pyravendb.connection.requests_handler import HttpRequestsHandler
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
        cls.requests_handler = HttpRequestsHandler(cls.default_url, cls.default_database)
        CreateDatabaseCommand(DatabaseDocument(cls.default_database, {"Raven/DataDir": "test"}),
                              cls.requests_handler).create_request()

        cls.index_map = ("from doc in docs "
                         "select new{"
                         "Tag = doc[\"@metadata\"][\"Raven-Entity-Name\"],"
                         "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],"
                         "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks}"
                         )
        cls.index = IndexDefinition("Testing", cls.index_map)
        PutIndexesCommand(cls.requests_handler, cls.index)

    @classmethod
    def tearDownClass(cls):
        DeleteDatabaseCommand("NorthWindTest", cls.requests_handler, True)
