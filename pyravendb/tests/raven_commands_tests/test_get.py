import unittest
from pyravendb.commands.raven_commands import PutDocumentCommand, GetDocumentCommand
from pyravendb.tests.test_base import TestBase


class TestGet(TestBase):
    def setUp(self):
        super(TestGet, self).setUp()
        put_command = PutDocumentCommand("products/101", {"Name": "test", "@metadata": {}})
        put_command2 = PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}})
        self.requests_executor = self.store.get_request_executor()
        self.requests_executor.execute(put_command)
        self.requests_executor.execute(put_command2)
        self.response = self.requests_executor.execute(GetDocumentCommand("products/101"))
        self.other_response = self.requests_executor.execute(GetDocumentCommand("products/10"))

    def tearDown(self):
        super(TestGet, self).tearDown()
        self.delete_all_topology_files()

    def test_equal(self):
        self.assertEqual(self.response["Results"][0]["@metadata"]["@id"], "products/101")

    def test_not_equal(self):
        self.assertNotEqual(self.response["Results"][0]["@metadata"]["@id"],
                            self.other_response["Results"][0]["@metadata"]["@id"])

    def test_null(self):
        self.assertIsNone(self.requests_executor.execute(GetDocumentCommand("product")))


if __name__ == "__main__":
    unittest.main()
