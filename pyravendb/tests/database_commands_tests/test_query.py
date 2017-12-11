from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery
from pyravendb.tests.test_base import TestBase
import time


class TestQuery(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestQuery, cls).setUpClass()
        cls.db.put("products/10", {"Name": "test"}, {"Raven-Entity-Name": "Products", "Raven-Python-Type": "object"})
        cls.db.put("products/11", {"Name": "test2"}, {"Raven-Entity-Name": "Products", "Raven-Python-Type": "object"})
        cls.db.put("products/12", {"Name": "test3"}, {"Raven-Entity-Name": "Products", "Raven-Python-Type": "object"})

    def test_only_query(self):
        self.db.put_index("Testing", self.index, True)
        response = self.db.query("Testing", IndexQuery("Tag:Products"))
        self.assertEqual(response["Results"][0]["Name"], "test")

    def test_get_only_metadata(self):
        self.db.put_index("Testing", self.index, True)
        response = self.db.query("Testing", IndexQuery("Tag:Products"), metadata_only=True)
        self.assertFalse("Name" in response["Results"][0])

    def test_get_only_index_entries(self):
        self.db.put_index("Testing", self.index, True)
        response = self.db.query("Testing", IndexQuery("Tag:Products"), index_entries_only=True)
        self.assertFalse("@metadata" in response["Results"][0])

    def test_fail(self):
        with self.assertRaises(ValueError):
            self.db.query(None, IndexQuery("Tag:Products"))

    def test_fail_none_response(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            self.db.query("IndexIsNotExists", IndexQuery("Tag:Products"))

    def test_only_query_change_page_size_and_add_start(self):
        self.db.put_index("Testing", self.index, True)
        start = time.time()
        while True and time.time() - start < 30:
            response = self.db.query("Testing",
                                     IndexQuery("Tag:Products", page_size=1, start=1))
            if not response["IsStale"]:
                break
        self.assertEqual(response["Results"][0]["Name"], "test2")
