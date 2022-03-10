from ravendb.tests.test_base import TestBase
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.tools.utils import Utils
import json


class TestMetadataAsDictionary(TestBase):
    def create(self):
        return MetadataAsDictionary(self.source)

    def setUp(self):
        super(TestMetadataAsDictionary, self).setUp()
        self.source = {
            "@collection": "Users",
            "Raven-Python-Type": "test_base.User",
            "@nested-object-types": {
                "people[]:": "User",
                "SignedUpAt": "date",
            },
        }
        self.metadata = self.create()

    def test_when_dumped_to_string_it_looks_like_source_object(self):
        self.assertEqual(
            json.dumps(self.source, default=Utils.json_default),
            json.dumps(self.metadata, default=Utils.json_default),
        )

    def test_gets_proper_data(self):
        self.assertEqual(3, len(self.metadata))
        self.assertEqual("test_base.User", self.metadata["Raven-Python-Type"])
        self.assertEqual("Users", self.metadata["@collection"])

    def test_gets_proper_data_for_nested_objects(self):
        self.assertIsNotNone(self.metadata["@nested-object-types"]._parent)
        self.assertEqual("@nested-object-types", self.metadata["@nested-object-types"]._parent_key)
        self.assertEqual(
            json.dumps(self.metadata, default=Utils.json_default),
            json.dumps(self.metadata["@nested-object-types"]._parent, default=Utils.json_default),
        )

    def test_sets_data(self):
        self.metadata = self.create()
        self.metadata["@collection"] = "Magic"
        self.assertEqual("Magic", self.metadata["@collection"])

    def test_updates_dirty_flag(self):
        self.metadata = self.create()
        self.assertEqual(False, self.metadata.is_dirty)
        self.metadata["@collection"] = "Magic"
        self.assertEqual(True, self.metadata.is_dirty)

    def test_sets_data_and_updates_dirty_flag_for_nested_objects(self):
        self.metadata = self.create()
        nested = self.metadata["@nested-object-types"]
        self.assertEqual(False, nested.is_dirty)
        nested["SignedUpAt"] = "new_type"
        self.assertEqual("new_type", self.metadata["@nested-object-types"]["SignedUpAt"])
        self.assertEqual(True, nested.is_dirty)
        self.assertEqual(False, self.metadata.is_dirty)
