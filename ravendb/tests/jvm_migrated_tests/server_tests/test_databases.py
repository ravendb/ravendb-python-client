from ravendb import GetDatabaseRecordOperation
from ravendb.documents.indexes.definitions import (
    IndexType,
    IndexPriority,
    FieldStorage,
    AutoFieldIndexing,
    AggregationOperation,
    GroupByArrayBehavior,
)
from ravendb.infrastructure.graph import Genre
from ravendb.serverwide.database_record import DatabaseRecordWithEtag
from ravendb.tests.test_base import TestBase


class DatabasesTest(TestBase):
    def setUp(self):
        super(DatabasesTest, self).setUp()

    def test_can_get_info_auto_index_info(self):
        self._create_movies_data(self.store)

        with self.store.open_session() as session:
            list(session.query(object_type=Genre).where_equals("name", "Fantasy"))

        record: DatabaseRecordWithEtag = self.store.maintenance.server.send(
            GetDatabaseRecordOperation(self.store.database)
        )
        self.assertEqual(1, len(record.auto_indexes))
        self.assertIn("Auto/Genres/Byname", record.auto_indexes)

        auto_index_definition = record.auto_indexes.get("Auto/Genres/Byname")
        self.assertIsNotNone(auto_index_definition)

        self.assertEqual(IndexType.AUTO_MAP, auto_index_definition.type)
        self.assertEqual("Auto/Genres/Byname", auto_index_definition.name)
        self.assertEqual(IndexPriority.NORMAL, auto_index_definition.priority)
        self.assertEqual("Genres", auto_index_definition.collection)
        self.assertEqual(1, len(auto_index_definition.map_fields))
        self.assertEqual(0, len(auto_index_definition.group_by_fields))

        field_options = auto_index_definition.map_fields.get("name")

        self.assertEqual(FieldStorage.NO, field_options.storage)
        self.assertEqual(AutoFieldIndexing.DEFAULT, field_options.indexing)
        self.assertEqual(AggregationOperation.NONE, field_options.aggregation)
        self.assertIsNone(field_options.spatial)
        self.assertEqual(GroupByArrayBehavior.NOT_APPLICABLE, field_options.group_by_array_behavior)
        self.assertFalse(field_options.suggestions)
        self.assertFalse(field_options.is_name_quoted)
