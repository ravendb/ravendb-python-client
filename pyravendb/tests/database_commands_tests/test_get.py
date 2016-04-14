import unittest

from pyravendb.tests.test_base import TestBase


class TestGet(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestGet, cls).setUpClass()
        cls.db.put("products/101", {"Name": "test"}, {})
        cls.db.put("products/10", {"Name": "test"}, {})
        cls.response = cls.db.get("products/101")
        cls.other_response = cls.db.get("products/10")

    def test_equal(self):
        self.assertEqual(self.response["Results"][0]["@metadata"]["@id"], "products/101")

    def test_not_equal(self):
        self.assertNotEqual(self.response["Results"][0]["@metadata"]["@id"],
                            self.other_response["Results"][0]["@metadata"]["@id"])

    def test_null(self):
        self.assertIsNone(self.db.get("product")["Results"][0])


if __name__ == "__main__":
    unittest.main()
