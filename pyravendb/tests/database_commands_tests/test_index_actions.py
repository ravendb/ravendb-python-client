import unittest

from pyravendb.custom_exceptions import exceptions
from pyravendb.tests.test_base import TestBase


class TestIndexActions(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestIndexActions, cls).setUpClass()

    def test_put_index_success(self):
        assert self.db.put_index("region", self.index, True)

    def test_put_index_fail(self):
        self.db.put_index("test", self.index, True)
        with self.assertRaises(exceptions.InvalidOperationException):
            self.db.put_index("test", self.index, False)

    def test_get_index_success(self):
        self.db.put_index("test", self.index, True)
        self.assertIsNotNone(self.db.get_index("test"))

    def test_get_index_fail(self):
        self.assertIsNone(self.db.get_index("reg"))

    def test_delete_index_success(self):
        self.db.put_index("test", self.index, True)
        self.assertIsNone(self.db.delete_index("delete"))

    def test_delete_index_fail(self):
        with self.assertRaises(ValueError):
            self.db.delete_index(None)


if __name__ == "__main__":
    unittest.main()
