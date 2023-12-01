from datetime import datetime

from ravendb.infrastructure.orders import Order, Company
from ravendb.tests.test_base import TestBase


class TestRavenDB15000(TestBase):
    def setUp(self):
        super(TestRavenDB15000, self).setUp()

    def test_can_include_time_series_without_providing_from_and_to_dates_via_load(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.store(Company(name="HR"), "companies/1-A")
            session.time_series_for("orders/1-A", "Heartrate").append_single(datetime(1999, 11, 14), 1)
            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                "orders/1-A", Order, lambda i: i.include_documents("company").include_time_series("Heartrate")
            )

            # should not go to server
            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("HR", company.name)

            # should not go to server
            vals = session.time_series_for_entity(order, "Heartrate").get()

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(1, len(vals))

    def test_can_include_time_series_without_providing_from_and_to_dates_via_query(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.store(Company(name="HR"), "companies/1-A")
            session.time_series_for("orders/1-A", "Heartrate").append_single(datetime(1999, 11, 14), 1)
            session.save_changes()

        with self.store.open_session() as session:
            order = (
                session.query(object_type=Order)
                .include(lambda i: i.include_documents("company").include_time_series("Heartrate"))
                .first()
            )

            # should not go to server
            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("HR", company.name)

            # should not go to server
            vals = session.time_series_for_entity(order, "Heartrate").get()

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(1, len(vals))
