from datetime import datetime

from ravendb import GetTermsOperation
from ravendb.documents.indexes.index_creation import AbstractJavaScriptIndexCreationTask
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class Companies_ByTimeSeriesNames(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(Companies_ByTimeSeriesNames, self).__init__()
        self.maps = ["map('Companies', function (company) {return ({names: timeSeriesNamesFor(company)})})"]


company_id = "companies/1"
base_line = datetime(2023, 8, 20, 21, 30)


class TestBasicTimeSeriesIndexesJavaScript(TestBase):
    def setUp(self):
        super(TestBasicTimeSeriesIndexesJavaScript, self).setUp()

    def test_time_series_names_for(self):
        now = RavenTestHelper.utc_today()
        index = Companies_ByTimeSeriesNames()
        index.execute(self.store)

        with self.store.open_session() as session:
            session.store(Company(), company_id)
            session.save_changes()

        self.wait_for_indexing(self.store)
        RavenTestHelper.assert_no_index_errors(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index.index_name, "names", None))
        self.assertEqual(0, len(terms))

        terms = self.store.maintenance.send(GetTermsOperation(index.index_name, "names_IsArray", None))
        self.assertEqual(1, len(terms))
        self.assertIn("true", terms)

        with self.store.open_session() as session:
            company = session.load(company_id, Company)
            session.time_series_for_entity(company, "heartRate").append_single(now, 2.5, "tag1")
            session.time_series_for_entity(company, "heartRate2").append_single(now, 3.5, "tag2")
            session.save_changes()

        self.wait_for_indexing(self.store)

        RavenTestHelper.assert_no_index_errors(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index.index_name, "names", None))
        self.assertEqual(2, len(terms))
        self.assertIn("heartrate", terms)
        self.assertIn("heartrate2", terms)

        terms = self.store.maintenance.send(GetTermsOperation(index.index_name, "names_IsArray", None))
        self.assertEqual(1, len(terms))
        self.assertIn("true", terms)
