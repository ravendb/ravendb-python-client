from pyravendb.connection import requests_executor, cluster_requests_executor
from pyravendb.data.indexes import IndexDefinition
import unittest
import sys
import os

sys.path.append(os.path.abspath(__file__ + "/../../"))

from pyravendb.data.database import DatabaseDocument
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.d_commands.raven_commands import CreateDatabaseCommand, DeleteDatabaseCommand, PutIndexesCommand


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.default_urls = ["http://localhost.fiddler:8080"]
        cls.default_database = "NorthWindTest"
        cls.requests_executor = requests_executor.RequestsExecutor.create(cls.default_urls, cls.default_database, None)
        cls.cluster_requests_executor = cluster_requests_executor.ClusterRequestExecutor.create(cls.default_urls, None)
        cls.cluster_requests_executor.execute(
            CreateDatabaseCommand(DatabaseDocument(cls.default_database, {"Raven/DataDir": "test"})))

        cls.index_map = ("from doc in docs "
                         "select new{"
                         "Tag = doc[\"@metadata\"][\"@collection\"],"
                         "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],"
                         "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks}"
                         )
        cls.index = IndexDefinition(name="Testing", index_map=cls.index_map)
        cls.requests_executor.execute(PutIndexesCommand(cls.index))

    @classmethod
    def tearDownClass(cls):
        cls.cluster_requests_executor.execute(DeleteDatabaseCommand("NorthWindTest", True))
