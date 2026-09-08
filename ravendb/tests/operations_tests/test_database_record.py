"""
Tests for DatabaseRecord serialization, in particular the fields whose C# counterparts
have no initializer and so are simply absent from a server payload.
"""

import unittest

from ravendb.documents.indexes.definitions import AutoIndexDefinition
from ravendb.serverwide.database_record import DatabaseRecord

AUTO_INDEX = {
    "Type": "AutoMap",
    "Name": "Auto/Orders/ByCompany",
    "Priority": "Normal",
    "State": "Normal",
    "Collection": "Orders",
    "MapFields": {},
    "GroupByFields": {},
}


class TestDatabaseRecordFromJson(unittest.TestCase):
    def test_a_payload_without_optional_sections_parses(self):
        # The C# fields for these have no initializer, so a server is free to omit them.
        record = DatabaseRecord.from_json({"DatabaseName": "db"})

        self.assertEqual("db", record.database_name)
        self.assertIsNone(record.auto_indexes)
        self.assertIsNone(record.indexes)

    def test_a_record_without_a_lock_mode_is_unlocked(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db"})

        self.assertEqual(DatabaseRecord.DatabaseLockMode.UNLOCK, record.lock_mode)

    def test_a_lock_mode_is_read_when_the_server_sends_one(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "LockMode": "PreventDeletesError"})

        self.assertEqual(DatabaseRecord.DatabaseLockMode.PREVENT_DELETES_ERROR, record.lock_mode)

    def test_auto_indexes_are_parsed_into_definitions(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "AutoIndexes": {"Auto/Orders/ByCompany": AUTO_INDEX}})

        self.assertIsInstance(record.auto_indexes["Auto/Orders/ByCompany"], AutoIndexDefinition)
        self.assertEqual("Orders", record.auto_indexes["Auto/Orders/ByCompany"].collection)

    def test_an_empty_auto_index_map_reads_as_no_auto_indexes(self):
        self.assertIsNone(DatabaseRecord.from_json({"DatabaseName": "db", "AutoIndexes": {}}).auto_indexes)


class TestDatabaseRecordToJson(unittest.TestCase):
    def test_auto_indexes_are_serialized_by_name(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "AutoIndexes": {"Auto/Orders/ByCompany": AUTO_INDEX}})
        serialized = record.to_json()["AutoIndexes"]

        self.assertEqual(["Auto/Orders/ByCompany"], list(serialized))
        self.assertEqual("Orders", serialized["Auto/Orders/ByCompany"]["Collection"])
        self.assertEqual("AutoMap", serialized["Auto/Orders/ByCompany"]["Type"])

    def test_a_record_with_no_auto_indexes_writes_none(self):
        self.assertIsNone(DatabaseRecord("db").to_json()["AutoIndexes"])

    def test_a_parsed_record_survives_a_serialization_round_trip(self):
        record = DatabaseRecord.from_json({"DatabaseName": "db", "AutoIndexes": {"Auto/Orders/ByCompany": AUTO_INDEX}})
        round_tripped = DatabaseRecord.from_json(record.to_json())

        self.assertEqual(
            record.auto_indexes["Auto/Orders/ByCompany"].collection,
            round_tripped.auto_indexes["Auto/Orders/ByCompany"].collection,
        )
