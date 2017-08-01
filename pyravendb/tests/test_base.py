from pyravendb.data.indexes import IndexDefinition
import unittest
import sys
import os

sys.path.append(os.path.abspath(__file__ + "/../../"))

from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation, DeleteDatabaseOperation
from pyravendb.commands.raven_commands import CreateDatabaseCommand, DeleteDatabaseCommand, PutIndexesCommand


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.default_urls = ["http://localhost.fiddler:8080"]
        cls.default_database = "NorthWindTest"
        cls.store = DocumentStore(urls=cls.default_urls, database=cls.default_database)
        cls.store.admin.server(CreateDatabaseOperation(database_name=cls.default_database))
        # cls.requests_executor = requests_executor.RequestsExecutor.create(cls.default_urls, cls.default_database, None)
        # cls.cluster_requests_executor = cluster_requests_executor.ClusterRequestExecutor.create(cls.default_urls, None)
        # cls.cluster_requests_executor.execute(
        #     CreateDatabaseCommand(DatabaseDocument(cls.default_database, {"Raven/DataDir": "test"})))
        #
        # cls.index_map = ("from doc in docs "
        #                  "select new{"
        #                  "Tag = doc[\"@metadata\"][\"@collection\"],"
        #                  "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],"
        #                  "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks}"
        #                  )
        # cls.index = IndexDefinition(name="Testing", index_map=cls.index_map)
        # cls.requests_executor.execute(PutIndexesCommand(cls.index))

    @classmethod
    def tearDownClass(cls):
        cls.store.admin.server(DeleteDatabaseOperation(database_name=cls.default_database, hard_delete=True))
