from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery
from pyravendb.tests.test_base import TestBase
from pyravendb.d_commands.raven_commands import *
from pyravendb.data.document_convention import DocumentConvention


class TestQuery(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestQuery, cls).setUpClass()
        cls.requests_executor.execute(
            PutDocumentCommand("products/10", {"Name": "test", "@metadata": {"@collection": "Products",
                                                                             "Raven-Python-Type": "object"}}))

    def test_only_query(self):
        self.requests_executor.execute(PutIndexesCommand(self.index))
        query_command = QueryCommand("Testing", IndexQuery("Tag:Products"), DocumentConvention())
        response = self.requests_executor.execute(query_command)
        self.assertEqual(response["Results"][0]["Name"], "test")

    def test_get_only_metadata(self):
        self.requests_executor.execute(PutIndexesCommand(self.index))
        query_command = QueryCommand("Testing", IndexQuery("Tag:Products"), conventions=DocumentConvention(),
                                     metadata_only=True)
        response = self.requests_executor.execute(query_command)
        self.assertFalse("Name" in response["Results"][0])

    def test_get_only_index_entries(self):
        self.requests_executor.execute(PutIndexesCommand(self.index))
        query_command = QueryCommand("Testing", IndexQuery("Tag:Products"), conventions=DocumentConvention(),
                                     index_entries_only=True)
        response = self.requests_executor.execute(query_command)
        self.assertFalse("@metadata" in response["Results"][0])

    def test_fail(self):
        with self.assertRaises(ValueError):
            query_command = QueryCommand(None, IndexQuery("Tag:Products"), conventions=DocumentConvention())
            self.requests_executor.execute(query_command)

    def test_fail_none_response(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            query_command = QueryCommand("IndexIsNotExists",
                                         IndexQuery("Tag:Products"), conventions=DocumentConvention())
            self.requests_executor.execute(query_command)
