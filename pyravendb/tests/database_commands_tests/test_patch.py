import unittest

from pyravendb.custom_exceptions.exceptions import DocumentDoesNotExistsException
from pyravendb.data.patches import ScriptedPatchRequest
from pyravendb.tests.test_base import TestBase


class TestPatch(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestPatch, cls).setUpClass()
        cls.db.put("products/10", {"Name": "test"}, {})

    def test_patch_success_ignore_missing(self):
        self.assertIsNotNone(self.db.patch("products/10", ScriptedPatchRequest("this.Name = 'testing'")))

    def test_patch_success_not_ignore_missing(self):
        self.assertIsNotNone(
            self.db.patch("products/10", ScriptedPatchRequest("this.Name = 'testing'"), ignore_missing=False))

    def test_patch_fail_not_ignore_missing(self):
        with self.assertRaises(DocumentDoesNotExistsException):
            self.db.patch("products/1", ScriptedPatchRequest("this.Name = 'testing'"), ignore_missing=False)


if __name__ == "__main__":
    unittest.main
