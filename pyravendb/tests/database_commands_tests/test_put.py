import unittest
from pyravendb.tests.test_base import TestBase
from pyravendb.d_commands.raven_commands import PutDocumentCommand, GetDocumentCommand


class TestDelete(TestBase):
    def test_put_success(self):
        PutDocumentCommand(key="testing/1", document={"Name": "test"}, metadata=None,
                           request_handler=self.requests_handler).create_request()
        response = GetDocumentCommand("testing/1", self.requests_handler).create_request()
        self.assertEqual(response["Results"][0]["@metadata"]["@id"], "testing/1")

    def test_put_fail(self):
        with self.assertRaises(ValueError):
            PutDocumentCommand(key="testing/2", document="document", metadata=None,
                               request_handler=self.requests_handler).create_request()

if __name__ == "__main__":
    unittest.main()
