import unittest
from typing import Optional

from pyravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from pyravendb.documents.session.loaders.include import QueryIncludeBuilder
from pyravendb.documents.session.misc import TransactionMode, SessionOptions
from pyravendb.documents.session.query import QueryStatistics
from pyravendb.infrastructure.orders import Company, Address, Employee
from pyravendb.tests.test_base import TestBase
from pyravendb.util.util import StartingWithOptions


class Companies_ByName(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from c in docs.Companies select new { name = c.name }"


class TestRavenDB14006(TestBase):
    def setUp(self):
        super().setUp()

    def test_compare_exchange_value_tracking_in_session_starts_with(self):
        all_companies = []
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        with self.store.open_session(session_options=session_options) as session:
            for i in range(10):
                company = Company(f"companies/{i}", "companies/hr", "HR")
                all_companies.append(company.Id)
                session.advanced.cluster_transaction.create_compare_exchange_value(company.Id, company)

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            results = session.advanced.cluster_transaction.get_compare_exchange_values(
                StartingWithOptions("comp"), Company
            )
            self.assertEqual(10, len(results))
            self.assertTrue(all(map(lambda x: x is not None, results)))
            self.assertEqual(1, session.number_of_requests)

            results = session.advanced.cluster_transaction.get_compare_exchange_values(all_companies, Company)

            self.assertEqual(10, len(results))
            self.assertTrue(all(map(lambda x: x is not None, results)))
            self.assertEqual(1, session.number_of_requests)

            for company_id in all_companies:
                result = session.advanced.cluster_transaction.get_compare_exchange_value(company_id, Company)
                self.assertIsNotNone(result.value)
                self.assertEqual(1, session.number_of_requests)

    def test_compare_exchange_value_tracking_in_session(self):
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            company = Company("companies/1", "companies/cf", "CF")
            session.store(company)

            number_of_requests = session.number_of_requests

            address = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value(company.external_id, address)

            self.assertEqual(number_of_requests, session.number_of_requests)

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)

            self.assertEqual(number_of_requests, session.number_of_requests)

            self.assertEqual(address, value1.value)
            self.assertEqual(company.external_id, value1.key)
            self.assertEqual(0, value1.index)

            session.save_changes()

            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            self.assertEqual(address, value1.value)
            self.assertEqual(value1.key, company.external_id)
            self.assertGreater(value1.index, 0)

            value2 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)
            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            self.assertEqual(value1, value2)

            session.save_changes()

            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            session.clear()

            value3 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)
            self.assertNotEqual(value2, value3)

        with self.store.open_session(session_options=session_options) as session:
            address = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address)

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            number_of_requests = session.number_of_requests

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value("companies/cf", Address)

            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            value2 = session.advanced.cluster_transaction.get_compare_exchange_value("companies/hr", Address)

            self.assertEqual(number_of_requests + 2, session.number_of_requests)

            values = session.advanced.cluster_transaction.get_compare_exchange_values(
                ["companies/cf", "companies/hr"], Address
            )

            self.assertEqual(number_of_requests + 2, session.number_of_requests)

            self.assertEqual(2, len(values))
            self.assertEqual(value1, values.get(value1.key))
            self.assertEqual(value2, values.get(value2.key))

            values = session.advanced.cluster_transaction.get_compare_exchange_values(
                ["companies/cf", "companies/hr", "companies/hx"], Address
            )

            self.assertEqual(number_of_requests + 3, session.number_of_requests)

            self.assertEqual(3, len(values))
            self.assertEqual(value1, values.get(value1.key))
            self.assertEqual(value2, values.get(value2.key))

            value3 = session.advanced.cluster_transaction.get_compare_exchange_value("companies/hx", Address)
            self.assertEqual(number_of_requests + 3, session.number_of_requests)

            self.assertIsNone(value3)
            self.assertIsNone(values.get("companies/hx", None))

            session.save_changes()

            self.assertEqual(number_of_requests + 3, session.number_of_requests)

            address = Address(city="Bydgoszcz")

            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hx", address)

            session.save_changes()

            self.assertEqual(number_of_requests + 4, session.number_of_requests)

    def test_can_use_compare_exchange_value_includes_in_queries_static_javascript(self):
        Companies_ByName().execute(self.store)
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            employee = Employee("employees/1", notes=["companies/cf", "companies/hr"])
            session.store(employee)

            company = Company("companies/1", "companies/cf", "CF")
            session.store(company)

            address1 = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/cf", address1)

            address2 = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address2)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session(session_options=session_options) as session:
            result_etag = None
            statistics: Optional[QueryStatistics] = None

            def __statistics_callback(stats: QueryStatistics):
                nonlocal statistics
                statistics = stats  # plug-in the reference, value will be changed

            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from index 'Companies/ByName' as c\n"
                    + "select incl(c)",
                ).statistics(__statistics_callback)
            )
            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            number_of_requests = session.number_of_requests
            result_etag = statistics.result_etag

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Torun", value1.value.city)

            self.assertEqual(number_of_requests, session.number_of_requests)

            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from index 'Companies/ByName' as c\n"
                    + "select incl(c)",
                ).statistics(__statistics_callback)
            )
            self.assertEqual(1, len(companies))
            self.assertEqual(-1, statistics.duration_in_ms)
            self.assertEqual(result_etag, statistics.result_etag)
            result_etag = statistics.result_etag

            with self.store.open_session(session_options=session_options) as inner_session:
                value = inner_session.advanced.cluster_transaction.get_compare_exchange_value(
                    companies[0].external_id, Address
                )
                value.value.city = "Bydgoszcz"
                inner_session.save_changes()
                self.wait_for_indexing(self.store)

            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from index 'Companies/ByName' as c\n"
                    + "select incl(c)",
                ).statistics(__statistics_callback)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            self.assertNotEqual(result_etag, statistics.result_etag)

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Bydgoszcz", value1.value.city)

    def test_can_use_compare_exchange_value_includes_in_queries_dynamic_javascript(self):
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            employee = Employee("employees/1", notes=["companies/cf", "companies/hr"])
            session.store(employee)

            company = Company("companies/1", "companies/cf", "CF")
            session.store(company)

            address1 = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/cf", address1)

            address2 = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address2)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session(session_options=session_options) as session:
            result_etag = None
            statistics: Optional[QueryStatistics] = None

            def __statistics_callback(stats: QueryStatistics):
                nonlocal statistics
                statistics = stats  # plug-in the reference, value will be changed

            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from Companies as c\n"
                    + "select incl(c)",
                    Company,
                ).statistics(__statistics_callback)
            )
            self.assertEqual(1, len(companies))
            self.assertGreater(statistics.duration_in_ms, 0)
            number_of_requests = session.number_of_requests
            result_etag = statistics.result_etag

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Torun", value1.value.city)

            self.assertEqual(number_of_requests, session.number_of_requests)
            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from Companies as c\n"
                    + "select incl(c)",
                    Company,
                ).statistics(__statistics_callback)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual(-1, statistics.duration_in_ms)
            self.assertEqual(result_etag, statistics.result_etag)

            with self.store.open_session(session_options=session_options) as inner_session:
                value = inner_session.advanced.cluster_transaction.get_compare_exchange_value(
                    companies[0].external_id, Address
                )
                value.value.city = "Bydgoszcz"
                inner_session.save_changes()

            companies = list(
                session.advanced.raw_query(
                    "declare function incl(c) {\n"
                    + "    includes.cmpxchg(c.external_id);\n"
                    + "    return c;\n"
                    + "}\n"
                    + "from Companies as c\n"
                    + "select incl(c)",
                    Company,
                ).statistics(__statistics_callback)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            self.assertNotEqual(result_etag, statistics.result_etag)

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Bydgoszcz", value1.value.city)

    def test_compare_exchange_value_tracking_in_session_no_tracking(self):
        company = Company("companies/1", "companies/cf", "CF")
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            session.store(company)

            address = Address()
            address.city = "Torun"
            session.advanced.cluster_transaction.create_compare_exchange_value(company.external_id, address)
            session.save_changes()

        session_options_no_tracking = SessionOptions()
        session_options_no_tracking.no_tracking = True
        session_options_no_tracking.transaction_mode = TransactionMode.CLUSTER_WIDE

        with self.store.open_session(session_options=session_options_no_tracking) as session:
            number_of_requests = session.number_of_requests
            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)

            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            value2 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)
            self.assertEqual(number_of_requests + 2, session.number_of_requests)

            self.assertNotEqual(value1, value2)

            value3 = session.advanced.cluster_transaction.get_compare_exchange_value(company.external_id, Address)

            self.assertEqual(number_of_requests + 3, session.number_of_requests)
            self.assertNotEqual(value2, value3)

        with self.store.open_session(session_options=session_options_no_tracking) as session:
            number_of_requests = session.number_of_requests

            value1 = session.advanced.cluster_transaction.get_compare_exchange_values_starting_with(
                company.external_id, object_type=Address
            )

            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            value2 = session.advanced.cluster_transaction.get_compare_exchange_values_starting_with(
                company.external_id, object_type=Address
            )

            self.assertEqual(number_of_requests + 2, session.number_of_requests)
            self.assertNotEqual(value1, value2)

            value3 = session.advanced.cluster_transaction.get_compare_exchange_values_starting_with(
                company.external_id, object_type=Address
            )

            self.assertEqual(number_of_requests + 3, session.number_of_requests)
            self.assertNotEqual(value2, value3)

        with self.store.open_session(session_options=session_options_no_tracking) as session:
            number_of_requests = session.number_of_requests

            value1 = session.advanced.cluster_transaction.get_compare_exchange_values([company.external_id], Address)
            self.assertEqual(number_of_requests + 1, session.number_of_requests)

            value2 = session.advanced.cluster_transaction.get_compare_exchange_values([company.external_id], Address)
            self.assertEqual(number_of_requests + 2, session.number_of_requests)

            self.assertNotEqual(value1.get(company.external_id), value2.get(company.external_id))

            value3 = session.advanced.cluster_transaction.get_compare_exchange_values([company.external_id], Address)

            self.assertEqual(number_of_requests + 3, session.number_of_requests)

            self.assertNotEqual(value2.get(company.external_id), value3.get(company.external_id))

    def test_can_use_compare_exchange_value_includes_in_queries_dynamic(self):
        session_options = SessionOptions()
        session_options.transaction_mode = TransactionMode.CLUSTER_WIDE

        with self.store.open_session(session_options=session_options) as session:
            employee = Employee("employees/1", notes=["companies/cf", "companies/hr"])
            session.store(employee)

            company = Company("companies/1", "companies/cf", "CF")
            session.store(company)

            address1 = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/cf", address1)

            address2 = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address2)

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            statistics: Optional[QueryStatistics] = None

            def __statistics_callback(stats: QueryStatistics):
                nonlocal statistics
                statistics = stats

            def __include_cmpxch(builder: QueryIncludeBuilder):
                builder.include_compare_exchange_value("external_id")

            companies = list(
                session.query(object_type=Company).statistics(__statistics_callback).include(__include_cmpxch)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            result_etag = statistics.result_etag

            number_of_requests = session.number_of_requests

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Torun", value1.value.city)

            self.assertEqual(number_of_requests, session.number_of_requests)

            companies = list(
                session.query(object_type=Company).statistics(__statistics_callback).include(__include_cmpxch)
            )

            self.assertEqual(1, len(companies))
            self.assertEqual(-1, statistics.duration_in_ms)
            self.assertEqual(result_etag, statistics.result_etag)

            with self.store.open_session(session_options=session_options) as inner_session:
                value = inner_session.advanced.cluster_transaction.get_compare_exchange_value(
                    companies[0].external_id, Address
                )
                value.value.city = "Bydgoszcz"

                inner_session.save_changes()

            companies = list(
                session.query(object_type=Company).statistics(__statistics_callback).include(__include_cmpxch)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            self.assertNotEqual(result_etag, statistics.result_etag)

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Bydgoszcz", value1.value.city)

    def test_can_use_compare_exchange_value_includes_in_queries_static(self):
        Companies_ByName().execute(self.store)

        session_options = SessionOptions()
        session_options.transaction_mode = TransactionMode.CLUSTER_WIDE

        with self.store.open_session(session_options=session_options) as session:
            employee = Employee("employees/1", notes=["companies/cf", "companies/hr"])
            session.store(employee)

            company = Company("companies/1", "companies/cf", "CF")
            session.store(company)

            address1 = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/cf", address1)

            address2 = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address2)

            session.save_changes()

        self.wait_for_indexing(self.store)

        def __statistics_callback(stats: QueryStatistics):
            nonlocal statistics
            statistics = stats

        def __include_cmpxch(builder: QueryIncludeBuilder):
            builder.include_compare_exchange_value("external_id")

        with self.store.open_session(session_options=session_options) as session:
            statistics: Optional[QueryStatistics] = None
            companies = list(
                session.query_index_type(Companies_ByName, Company)
                .statistics(__statistics_callback)
                .include(__include_cmpxch)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            result_etag = statistics.result_etag

            number_of_requests = session.number_of_requests

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Torun", value1.value.city)

            self.assertEqual(number_of_requests, session.number_of_requests)

            companies = list(
                session.query_index_type(Companies_ByName, Company)
                .statistics(__statistics_callback)
                .include(__include_cmpxch)
            )
            self.assertEqual(1, len(companies))
            self.assertEqual(-1, statistics.duration_in_ms)
            self.assertEqual(result_etag, statistics.result_etag)

            with self.store.open_session(session_options=session_options) as inner_session:
                value = inner_session.advanced.cluster_transaction.get_compare_exchange_value(
                    companies[0].external_id, Address
                )
                value.value.city = "Bydgoszcz"

                inner_session.save_changes()

                self.wait_for_indexing(self.store)

            companies = list(
                session.query_index_type(Companies_ByName, Company)
                .statistics(__statistics_callback)
                .include(__include_cmpxch)
            )

            self.assertEqual(1, len(companies))
            self.assertGreaterEqual(statistics.duration_in_ms, 0)
            self.assertNotEqual(result_etag, statistics.result_etag)

            value1 = session.advanced.cluster_transaction.get_compare_exchange_value(companies[0].external_id, Address)
            self.assertEqual("Bydgoszcz", value1.value.city)
