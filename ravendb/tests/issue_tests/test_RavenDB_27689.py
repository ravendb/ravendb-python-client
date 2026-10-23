from threading import Event

from ravendb.documents.session.time_series import TimeSeriesRangeType
from ravendb.documents.subscriptions.options import SubscriptionCreationOptions
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.infrastructure.orders import Employee, Order
from ravendb.tests.test_base import TestBase


class TestRavenDB27689(TestBase):
    def _create_subscription_query(self, includes) -> str:
        name = self.store.subscriptions.create_for_class(Order, SubscriptionCreationOptions(includes=includes))
        return self.store.subscriptions.get_subscription_state(name).query

    def _assert_subscription_includes_employee_and_counters(self, includes):
        with self.store.open_session() as session:
            session.store(Employee(first_name="Anne"), "employees/1")
            session.store(Order(employee="employees/1"), "orders/1")
            session.counters_for("orders/1").increment("Pros", 3)
            session.counters_for("orders/1").increment("Cons", 1)
            session.save_changes()

        name = self.store.subscriptions.create_for_class(Order, SubscriptionCreationOptions(includes=includes))

        with self.store.subscriptions.get_subscription_worker_by_name(name, Order) as worker:
            event = Event()

            def __subscription_callback(batch: SubscriptionBatch[Order]):
                with batch.open_session() as session:
                    order = session.load("orders/1", Order)
                    self.assertIsNotNone(session.load(order.employee, Employee))
                    self.assertEqual(3, session.counters_for(order).get("Pros"))
                    self.assertEqual(1, session.counters_for(order).get("Cons"))
                    self.assertEqual(0, session.advanced.number_of_requests)

                event.set()

            worker.run(__subscription_callback)
            self.assertTrue(event.wait(30))

    def test_can_create_subscription_with_include_counters(self):
        query = self._create_subscription_query(lambda b: b.include_counters("Pros", "Cons"))
        self.assertEqual(1, query.count("include "))

    def test_can_create_subscription_with_include_multiple_time_series(self):
        query = self._create_subscription_query(
            lambda b: b.include_time_series_by_range_type_and_count(
                "Heartrate", TimeSeriesRangeType.LAST, 1
            ).include_time_series_by_range_type_and_count("Pulse", TimeSeriesRangeType.LAST, 1)
        )
        self.assertEqual(1, query.count("include "))

    def test_can_create_subscription_with_include_documents_and_counters(self):
        self._assert_subscription_includes_employee_and_counters(
            lambda b: b.include_documents("employee").include_counters("Pros", "Cons")
        )

    def test_can_create_subscription_with_include_documents_and_all_counters(self):
        self._assert_subscription_includes_employee_and_counters(
            lambda b: b.include_documents("employee").include_all_counters()
        )
