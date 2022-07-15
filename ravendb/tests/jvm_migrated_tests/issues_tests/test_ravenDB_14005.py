from ravendb import SessionOptions, TransactionMode
from ravendb.infrastructure.orders import Address
from ravendb.tests.test_base import TestBase


class TestRavenDB14005(TestBase):
    def setUp(self):
        super(TestRavenDB14005, self).setUp()

    def can_get_compare_exchange_values_lazily(self, no_tracking: bool):
        session_options = SessionOptions()
        session_options.no_tracking = no_tracking
        session_options.transaction_mode = TransactionMode.CLUSTER_WIDE
        with self.store.open_session(session_options=session_options) as session:
            lazy_value = session.advanced.cluster_transaction.lazily.get_compare_exchange_value("companies/hr", Address)
            self.assertFalse(lazy_value.is_value_created)

            address = lazy_value.value
            self.assertIsNone(address)

            value = session.advanced.cluster_transaction.get_compare_exchange_value("companies/hr", Address)
            self.assertEqual(value, address)

            lazy_values = session.advanced.cluster_transaction.lazily.get_compare_exchange_values(
                ["companies/hr", "companies/cf"], Address
            )

            self.assertFalse(lazy_values.is_value_created)

            addresses = lazy_values.value

            self.assertIsNotNone(addresses)
            self.assertEqual(2, len(addresses))
            self.assertIn("companies/hr", addresses.keys())
            self.assertIn("companies/cf", addresses.keys())

            self.assertIsNone(addresses["companies/hr"])
            self.assertIsNone(addresses["companies/cf"])

            values = session.advanced.cluster_transaction.get_compare_exchange_values(
                ["companies/hr", "companies/cf"], Address
            )
            self.assertIn("companies/hr", values.keys())
            self.assertIn("companies/cf", values.keys())
            self.assertEqual(addresses["companies/hr"], values["companies/hr"])
            self.assertEqual(addresses["companies/cf"], values["companies/cf"])

        with self.store.open_session(session_options=session_options):
            lazy_value = session.advanced.cluster_transaction.lazily.get_compare_exchange_value("companies/hr", Address)
            lazy_values = session.advanced.cluster_transaction.lazily.get_compare_exchange_values(
                ["companies/hr", "companies/cf"], Address
            )

            self.assertFalse(lazy_value.is_value_created)
            self.assertFalse(lazy_values.is_value_created)

            session.advanced.eagerly.execute_all_pending_lazy_operations()

            number_of_requests = session.advanced.number_of_requests

            self.assertEqual(number_of_requests, session.advanced.number_of_requests)

            self.assertIsNotNone(addresses)
            self.assertEqual(2, len(addresses))
            self.assertIn("companies/hr", addresses.keys())
            self.assertIn("companies/cf", addresses.keys())

            self.assertIsNone(addresses["companies/hr"])
            self.assertIsNone(addresses["companies/cf"])

        cluster_session_options = SessionOptions()
        cluster_session_options.transaction_mode = TransactionMode.CLUSTER_WIDE

        with self.store.open_session(session_options=cluster_session_options) as session:
            address1 = Address(city="Hadera")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/hr", address1)

            address2 = Address(city="Torun")
            session.advanced.cluster_transaction.create_compare_exchange_value("companies/cf", address2)

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            lazy_value = session.advanced.cluster_transaction.lazily.get_compare_exchange_value("companies/hr", Address)

            self.assertFalse(lazy_value.is_value_created)

            address = lazy_value.value
            self.assertIsNotNone(address)

            self.assertEqual("Hadera", address.value.city)
            value = session.advanced.cluster_transaction.get_compare_exchange_value("companies/hr", Address)
            self.assertEqual(value.value.city, address.value.city)

            lazy_values = session.advanced.cluster_transaction.lazily.get_compare_exchange_values(
                ["companies/hr", "companies/cf"], Address
            )

            self.assertFalse(lazy_values.is_value_created)

            addresses = lazy_values.value

            self.assertIsNotNone(addresses)
            self.assertEqual(2, len(addresses))
            self.assertIn("companies/hr", addresses.keys())
            self.assertIn("companies/cf", addresses.keys())

            self.assertEqual(addresses["companies/hr"].value.city, "Hadera")
            self.assertEqual(addresses["companies/cf"].value.city, "Torun")

            values = session.advanced.cluster_transaction.get_compare_exchange_values(
                ["companies/hr", "companies/cf"], Address
            )

            self.assertEqual(addresses["companies/hr"].value.city, values["companies/hr"].value.city)
            self.assertEqual(addresses["companies/cf"].value.city, values["companies/cf"].value.city)

    def test_can_get_compare_exchange_values_lazily_no_tracking(self):
        self.can_get_compare_exchange_values_lazily(False)

    def test_can_get_compare_exchange_values_lazily_with_tracking(self):
        self.can_get_compare_exchange_values_lazily(True)
