import unittest
from pyravendb.commands.raven_commands import PutDocumentCommand, GetDocumentCommand
from pyravendb.tests.test_base import TestBase


class TestGet(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestGet, cls).setUpClass()
        put_command = PutDocumentCommand("products/101", {"Name": "test","@metadata":{}})
        put_command2 = PutDocumentCommand("products/10", {"Name": "test", "@metadata": {}})
        cls.requests_executor.execute(put_command)
        cls.requests_executor.execute(put_command2)
        cls.response = cls.requests_executor.execute(GetDocumentCommand("products/101"))
        cls.other_response = cls.requests_executor.execute(GetDocumentCommand("products/10"))

    def test_equal(self):
        self.assertEqual(self.response["Results"][0]["@metadata"]["@id"], "products/101")

    def test_not_equal(self):
        self.assertNotEqual(self.response["Results"][0]["@metadata"]["@id"],
                            self.other_response["Results"][0]["@metadata"]["@id"])

    def test_null(self):
        self.assertIsNone(self.requests_executor.execute(GetDocumentCommand("product")))


if __name__ == "__main__":
    unittest.main()
