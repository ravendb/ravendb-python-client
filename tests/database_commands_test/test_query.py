import unittest

from data.indexes import IndexQuery
from tests.test_base import TestBase


class TestQuery(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestQuery, cls).setUpClass()
        cls.db.put("products/10", {"Name": "test"}, {"Raven-Entity-Name": "Products", "Raven-Python-Type": "object"})

    def setUp(self):
        self.db.put_index("Testing", self.index, True)

    def test_only_query(self):
        response = self.db.query("Testing", IndexQuery("Tag:Products"))
        self.assertEqual(response["Results"][0]["Name"], "test")

    def test_get_only_metadata(self):
        response = self.db.query("Testing", IndexQuery("Tag:Products"), metadata_only=True)
        self.assertFalse("Name" in response["Results"][0])

    def test_get_only_index_entries(self):
        response = self.db.query("Testing", IndexQuery("Tag:Products"), index_entries_only=True)
        self.assertFalse("@metadata" in response["Results"][0])

    def test_fail(self):
        with self.assertRaises(ValueError):
            self.db.query(None, IndexQuery("Tag:Products"))

    def test_fail_none_response(self):
        self.assertIsNone(self.db.query("IndexIsNotExists", IndexQuery("Tag:Products")))


if __name__ == "__main__":
    unittest.main()
