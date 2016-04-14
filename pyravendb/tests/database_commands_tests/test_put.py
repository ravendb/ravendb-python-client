import unittest
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    def test_put_success(self):
        self.db.put("testing/1", {"Name": "test"}, None)
        response = self.db.get("testing/1")
        self.assertEqual(response["Results"][0]["@metadata"]["@id"], "testing/1")

    def test_put_fail(self):
        with self.assertRaises(ValueError):
            self.db.put("testing/2", "document", None)


if __name__ == "__main__":
    unittest.main()
