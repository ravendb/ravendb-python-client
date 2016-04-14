import unittest

from pyravendb.custom_exceptions import exceptions
from pyravendb.tests.test_base import TestBase


class TestDelete(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestDelete, cls).setUpClass()
        cls.response = cls.db.put("products/101", {"Name": "test"}, {})
        cls.db.put("products/10", {"Name": "test"}, {})
        cls.other_response = cls.db.put("products/102", {"Name": "test"}, {})

    def test_delete_success_no_etag(self):
        self.assertIsNone(self.db.delete("products/10"))

    def test_delete_fail(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            self.db.delete("products/101", self.response[0]["Etag"][0:len(self.response[0]["Etag"]) - 2] + "10")

    def test_delete_fail_with_etag(self):
        self.assertIsNone(self.db.delete("products/102", self.other_response[0]["Etag"]))

    if __name__ == "__main__":
        unittest.main()
