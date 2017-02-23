import unittest

from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexQuery
from pyravendb.data.patches import PatchRequest
from pyravendb.tests.test_base import TestBase


class TestByIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestByIndexActions, cls).setUpClass()
        cls.patch = PatchRequest("this.Name = 'testing';")
        cls.db.put("testing/1", {"Name": "test"}, {"Raven-Entity-Name": "Testing"})

    def test_update_by_index_success(self):
        self.assertIsNotNone(
            self.db.update_by_index("Raven/DocumentsByEntityName", IndexQuery("Name:test"), self.patch))

    def test_update_by_index_fail(self):
        with self.assertRaises(Exception):
            self.db.update_by_index("", IndexQuery("Name:test"), self.patch)

    def test_delete_by_index_fail(self):
        with self.assertRaises(exceptions.ErrorResponseException):
            self.db.delete_by_index("region2", IndexQuery("Name:Western"))

    def test_delete_by_index_success(self):
        self.assertIsNotNone(
            self.db.delete_by_index("Testing", IndexQuery("Name:test")))


if __name__ == "__main__":
    unittest.main()
