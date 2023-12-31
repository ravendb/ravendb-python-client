from datetime import datetime, timedelta

from ravendb import GetTermsOperation
from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractJavaScriptIndexCreationTask
from ravendb.documents.indexes.time_series import AbstractJavaScriptTimeSeriesIndexCreationTask
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company, Employee, Address
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

    def test_basic_map_index_with_load(self):
        now1 = datetime.utcnow()
        now2 = now1 + timedelta(seconds=1)

        with self.store.open_session() as session:
            employee = Employee(first_name="John")
            session.store(employee, "employees/1")
            company = Company()
            session.store(company, "companies/1")

            session.time_series_for_entity(company, "HeartRate").append_single(now1, 7.0, employee.Id)

            company2 = Company()
            session.store(company2, "companies/11")

            session.time_series_for_entity(company2, "HeartRate").append_single(now1, 11.0, employee.Id)

            session.save_changes()

        time_series_index = MyTsIndex_Load()
        index_name = time_series_index.index_name
        index_definition = time_series_index.create_index_definition()

        time_series_index.execute(self.store)

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "employee", None))
        self.assertEqual(1, len(terms))
        self.assertIn("john", terms)

        with self.store.open_session() as session:
            employee = session.load("employees/1", Employee)
            employee.first_name = "Bob"
            session.save_changes()

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "employee", None))
        self.assertEqual(1, len(terms))
        self.assertIn("bob", terms)

        with self.store.open_session() as session:
            session.delete("employees/1")
            session.save_changes()

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "employee", None))
        self.assertEqual(0, len(terms))

    def test_basic_map_reduce_index_with_load(self):
        today = RavenTestHelper.utc_today()

        with self.store.open_session() as session:
            address = Address()
            address.city = "NY"

            session.store(address, "addresses/1")

            user = User(address_id=address.Id)
            session.store(user, "users/1")

            for i in range(10):
                session.time_series_for_entity(user, "heartRate").append_single(
                    today + timedelta(hours=i), 180 + i, address.Id
                )

            session.save_changes()

        time_series_index = AverageHeartRateDaily_ByDateAndCity()
        index_name = time_series_index.index_name
        index_definition = time_series_index.create_index_definition()

        time_series_index.execute(self.store)

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "heart_beat", None))
        self.assertEqual(1, len(terms))
        self.assertIn("184.5", terms)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "date", None))
        self.assertEqual(1, len(terms))

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "city", None))
        self.assertEqual(1, len(terms))
        self.assertIn("ny", terms)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "count", None))
        self.assertEqual(1, len(terms))
        self.assertEqual("10", terms[0])

        with self.store.open_session() as session:
            address = session.load("addresses/1", Address)
            address.city = "LA"
            session.save_changes()

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "city", None))
        self.assertEqual(1, len(terms))
        self.assertIn("la", terms)

    def test_can_map_all_time_series_from_collection(self):
        now1 = datetime.utcnow()
        now2 = now1 + timedelta(seconds=1)

        with self.store.open_session() as session:
            company = Company()
            session.store(company, "companies/1")
            session.time_series_for_entity(company, "heartRate").append_single(now1, 7.0, "tag1")
            session.time_series_for_entity(company, "likes").append_single(now1, 3.0, "tag2")

            session.save_changes()

        MyTsIndex_AllTimeSeries().execute(self.store)
        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation("MyTsIndex/AllTimeSeries", "heart_beat", None))
        self.assertEqual(2, len(terms))
        self.assertIn("7", terms)
        self.assertIn("3", terms)

    def test_basic_multi_map_index(self):
        now = RavenTestHelper.utc_today()

        time_series_index = MyMultiMapTsIndex()
        time_series_index.execute(self.store)
        with self.store.open_session() as session:
            company = Company()
            session.store(company)

            session.time_series_for_entity(company, "heartRate").append_single(now, 2.5, "tag1")
            session.time_series_for_entity(company, "heartRate2").append_single(now, 3.5, "tag2")

            user = User()
            session.store(user)
            session.time_series_for_entity(user, "heartRate").append_single(now, 4.5, "tag3")

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(session.query_index_type(MyMultiMapTsIndex, MyMultiMapTsIndex.Result))
            self.assertEqual(3, len(results))


class MyTsIndex_Load(AbstractJavaScriptTimeSeriesIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.maps = [
            "timeSeries.map('Companies', 'HeartRate', function (ts) {\n"
            + "return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Value,\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        user: ts.DocumentId,\n"
            + "        employee: load(entry.Tag, 'Employees').first_name\n"
            + "    }));\n"
            + "})"
        ]


class MyTsIndex_AllTimeSeries(AbstractJavaScriptTimeSeriesIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.maps = [
            "timeSeries.map('Companies', function (ts) {\n"
            + " return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Values[0],\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        user: ts.documentId\n"
            + "    }));\n"
            + " })"
        ]


class AverageHeartRateDaily_ByDateAndCity(AbstractJavaScriptTimeSeriesIndexCreationTask):
    class Result:
        def __init__(self, heart_beat: float = None, date: datetime = None, city: str = None, count: int = None):
            self.heart_beat = heart_beat
            self.date = date
            self.city = city
            self.count = count

    def __init__(self):
        super(AverageHeartRateDaily_ByDateAndCity, self).__init__()
        self.maps = [
            "timeSeries.map('Users', 'heartRate', function (ts) {\n"
            + "return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Value,\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        city: load(entry.Tag, 'Addresses').city,\n"
            + "        count: 1\n"
            + "    }));\n"
            + "})"
        ]

        self.reduce = (
            "groupBy(r => ({ date: r.date, city: r.city }))\n"
            " .aggregate(g => ({\n"
            "     heart_beat: g.values.reduce((total, val) => val.heart_beat + total, 0) / g.values.reduce((total, val) => val.count + total, 0),\n"
            "     date: g.key.date,\n"
            "     city: g.key.city\n"
            "     count: g.values.reduce((total, val) => val.count + total, 0)\n"
            " }))"
        )


class MyMultiMapTsIndex(AbstractJavaScriptTimeSeriesIndexCreationTask):
    class Result:
        def __init__(self, heart_beat: float = None, date: datetime = None, user: str = None):
            self.heart_beat = heart_beat
            self.date = date
            self.user = user

    def __init__(self):
        super(MyMultiMapTsIndex, self).__init__()
        self.maps = set()
        self.maps.add(
            "timeSeries.map('Companies', 'HeartRate', function (ts) {\n"
            + "return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Values[0],\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        user: ts.DocumentId\n"
            + "    }));\n"
            + "})"
        )

        self.maps.add(
            "timeSeries.map('Companies', 'HeartRate2', function (ts) {\n"
            + "return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Values[0],\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        user: ts.DocumentId\n"
            + "    }));\n"
            + "})"
        )
        self.maps.add(
            "timeSeries.map('Users', 'HeartRate', function (ts) {\n"
            + "return ts.Entries.map(entry => ({\n"
            + "        heart_beat: entry.Values[0],\n"
            + "        date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),\n"
            + "        user: ts.DocumentId\n"
            + "    }));\n"
            + "})"
        )
