from pyravendb.documents.commands import PutDocumentCommand, GetDocumentsCommand
from pyravendb.tests.test_base import *


class TestPut(TestBase):
    def tearDown(self):
        super(TestPut, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_put_success(self):
        request_executor = self.store.get_request_executor()
        put_command = PutDocumentCommand("testing/1", None, {"Name": "test", "@metadata": {}})
        request_executor.execute_command(put_command)
        command = GetDocumentsCommand(GetDocumentsCommand.GetDocumentsByIdCommandOptions("testing/1"))
        request_executor.execute_command(command)
        response = command.result
        self.assertEqual(response.results[0]["@metadata"]["@id"], "testing/1")

    # todo: request executor error handling
    def test_put_fail(self):
        request_executor = self.store.get_request_executor()
        with self.assertRaises(ValueError):
            command = PutDocumentCommand("testing/2", None, "document")
            request_executor.execute_command(command)


if __name__ == "__main__":
    unittest.main()
