from pyravendb.tests.test_base import *
from pyravendb.commands.raven_commands import PutDocumentCommand, GetDocumentCommand


class TestPut(TestBase):
    def tearDown(self):
        super(TestPut, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_put_success(self):
        request_executor = self.store.get_request_executor()
        put_command = PutDocumentCommand(key="testing/1", document={"Name": "test", "@metadata": {}})
        request_executor.execute(put_command)
        response = request_executor.execute(GetDocumentCommand("testing/1"))
        self.assertEqual(response["Results"][0]["@metadata"]["@id"], "testing/1")

    def test_put_fail(self):
        request_executor = self.store.get_request_executor()
        with self.assertRaises(ValueError):
            command = PutDocumentCommand(key="testing/2", document="document")
            request_executor.execute(command)


if __name__ == "__main__":
    unittest.main()
