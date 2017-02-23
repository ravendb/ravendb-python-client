import unittest

from pyravendb.d_commands import commands_data
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestDelete, cls).setUpClass()
        cls.put_command1 = commands_data.PutCommandData("products/999",
                                                        document={"Name": "tests", "Category": "testing"},
                                                        metadata={"Raven-Python-Type": "Products"})
        cls.put_command2 = commands_data.PutCommandData("products/1000",
                                                        document={"Name": "testsDelete", "Category": "testing"},
                                                        metadata={"Raven-Python-Type": "Products"})
        cls.delete_command = commands_data.DeleteCommandData("products/1000")
        patch = PatchRequest("this.Name = 'testing';")
        cls.scripted_patch_command = commands_data.ScriptedPatchCommandData("products/999", patch)

    def test_success_one_command(self):
        batch_result = self.db.batch([self.put_command1])
        self.assertEqual(len(batch_result), 1)

    def test_success_multi_commands(self):
        batch_result = self.db.batch([self.put_command1, self.put_command2, self.delete_command])
        self.assertEqual(len(batch_result), 3)

    def test_scripted_patch(self):
        self.db.batch([self.put_command1, self.scripted_patch_command])
        result = self.db.get("products/999")
        self.assertEqual(result["Results"][0]["Name"], "testing")

    def test_fail(self):
        with self.assertRaises(ValueError):
            self.db.batch([self.put_command1, self.put_command2, None])

    if __name__ == "__main__":
        unittest.main()
