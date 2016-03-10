from connection.requests_factory import HttpRequestsFactory
from data.indexes import IndexDefinition
import unittest
import sys
import os

sys.path.append(os.path.abspath(__file__ + "/../../"))

from d_commands import database_commands
from data.database import DatabaseDocument

default_url = "http://localhost:8081"
default_database = "NorthWindTest"


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = database_commands.DatabaseCommands(HttpRequestsFactory(default_url, default_database))
        cls.db.admin_commands.create_database(DatabaseDocument("NorthWindTest", {"Raven/DataDir": "test"}))
        cls.maps = set()
        cls.maps.add(("from doc in docs"
                      "select new{"
                      "Tag = doc[\"@metadata\"][\"Raven-Entity-Name\"],"
                      "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],"
                      "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks}"
                      )
                     )
        cls.index = IndexDefinition(cls.maps)
        cls.db.put_index("Testing", cls.index, True)

    @classmethod
    def tearDownClass(cls):
        cls.db.admin_commands.delete_database("NorthWindTest", True)
