from pyravendb.data.compare_exchange import StartingWithOptions
from pyravendb.documents import SessionOptions
from pyravendb.documents.session import TransactionMode
from pyravendb.tests.test_base import TestBase


class Contact:
    def __init__(self, name: str = None, title: str = None):
        self.name = name
        self.title = title


class Address:
    def __init__(
        self,
        line1: str = None,
        line2: str = None,
        city: str = None,
        region: str = None,
        postal_code: str = None,
        country: str = None,
    ):
        self.line1 = line1
        self.line2 = line2
        self.city = city
        self.region = region
        self.postal_code = postal_code
        self.country = country


class Company:
    def __init__(
        self,
        Id: str = None,
        external_id: str = None,
        name: str = None,
        contact: Contact = None,
        address: Address = None,
        phone: str = None,
        fax: str = None,
    ):
        self.Id = Id
        self.external_id = external_id
        self.name = name
        self.contact = contact
        self.address = address
        self.phone = phone
        self.fax = fax


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
                Company, StartingWithOptions("comp")
            )
            self.assertEqual(10, len(results))
            self.assertTrue(all(map(lambda x: x is not None, results)))
            self.assertEqual(1, session.number_of_requests)

            results = session.advanced.cluster_transaction.get_compare_exchange_values(Company, all_companies)

            self.assertEqual(10, len(results))
            self.assertTrue(all(map(lambda x: x is not None, results)))
            self.assertEqual(1, session.number_of_requests)

            for company_id in all_companies:
                result = session.advanced.cluster_transaction.get_compare_exchange_value(Company, company_id)
                self.assertIsNotNone(result.value)
                self.assertEqual(1, session.number_of_requests)
