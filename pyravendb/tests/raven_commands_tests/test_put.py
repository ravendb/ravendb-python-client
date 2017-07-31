import unittest
from pyravendb.tests.test_base import TestBase
from pyravendb.commands.raven_commands import PutDocumentCommand, GetDocumentCommand


class TestDelete(TestBase):
    def test_put_success(self):
        put_command = PutDocumentCommand(key="testing/1", document={"Name": "test","@metadata":{}})
        self.requests_executor.execute(put_command)
        response =  self.requests_executor.execute(GetDocumentCommand("testing/1"))
        self.assertEqual(response["Results"][0]["@metadata"]["@id"], "testing/1")

    def test_put_fail(self):
        with self.assertRaises(ValueError):
            command = PutDocumentCommand(key="testing/2", document="document")
            self.requests_executor.execute(command)

if __name__ == "__main__":
    unittest.main()
