from datetime import datetime

from ravendb import PutIndexesOperation, GetTermsOperation
from ravendb.documents.indexes.time_series import TimeSeriesIndexDefinition
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestBasicTimeSeriesIndexes_MixedSyntax(TestBase):
    def setUp(self):
        super(TestBasicTimeSeriesIndexes_MixedSyntax, self).setUp()

    def test_basic_map_index(self):
        now1 = datetime.utcnow()

        with self.store.open_session() as session:
            company = Company()
            session.store(company, "companies/1")
            session.time_series_for_entity(company, "HeartRate").append_single(now1, 7, "tag")
            session.save_changes()

        time_series_index_definition = TimeSeriesIndexDefinition()
        time_series_index_definition.name = "MyTsIndex"
        time_series_index_definition.maps = [
            "from ts in timeSeries.Companies.HeartRate.Where(x => true) "
            + "from entry in ts.Entries "
            + "select new { "
            + "   heartBeat = entry.Values[0], "
            + "   date = entry.Timestamp.Date, "
            + "   user = ts.DocumentId "
            + "}"
        ]

        self.store.maintenance.send(PutIndexesOperation(time_series_index_definition))
        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation("MyTsIndex", "heartBeat", None))
        self.assertEqual(1, len(terms))
        self.assertIn("7", terms)
