import unittest

from ravendb.documents.operations.configuration import ClientConfiguration, PutClientConfigurationOperation
from ravendb.documents.operations.statistics import GetStatisticsOperation
from ravendb.documents.session.misc import SessionOptions, TransactionMode
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB13456(TestBase):
    def setUp(self):
        super().setUp()

    @unittest.skip("Fails on cicd due to free license - adding the client configuration is disallowed")
    def test_can_change_identity_parts_separator(self):
        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1)

            self.assertTrue(company1.Id.startswith("companies/1-A"))

            company2 = Company()
            session.store(company2)

            self.assertTrue(company2.Id.startswith("companies/2-A"))

        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1, "companies/")

            company2 = Company()
            session.store(company2, "companies|")

            session.save_changes()

            self.assertTrue(company1.Id.startswith("companies/000000000"))
            self.assertEqual("companies/1", company2.Id)

        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company:", Company())
            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company|", Company())
            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company| cannot end with '|' or '/' as part of cluster transaction",
            )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company/", Company())
            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company/ cannot end with '|' or '/' as part of cluster transaction",
            )

        client_configuration = ClientConfiguration()
        client_configuration.identity_parts_separator = ":"

        self.store.maintenance.send(PutClientConfigurationOperation(client_configuration))
        self.store.maintenance.send(GetStatisticsOperation())  # forcing client configuration update

        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1)

            self.assertTrue(company1.Id.startswith("companies:3-A"))

            company2 = Company()
            session.store(company2)

            self.assertTrue(company2.Id.startswith("companies:4-A"))

        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1, "companies:")

            company2 = Company()
            session.store(company2, "companies|")

            session.save_changes()

            self.assertTrue(company1.Id.startswith("companies:000000000"))
            self.assertEqual("companies:2", company2.Id)

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company:", Company())

            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company: cannot end with '|' or ':' as part of cluster transaction",
            )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company|", Company())

            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company| cannot end with '|' or ':' as part of cluster transaction",
            )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company/", Company())

            session.save_changes()

        second_client_configuration = ClientConfiguration()
        second_client_configuration.identity_parts_separator = None
        self.store.maintenance.send(PutClientConfigurationOperation(second_client_configuration))

        self.store.maintenance.send(GetStatisticsOperation())  # forcing client configuration update

        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1)

            self.assertTrue(company1.Id.startswith("companies/5-A"))

            company2 = Company()
            session.store(company2)

            self.assertTrue(company2.Id.startswith("companies/6-A"))

        with self.store.open_session() as session:
            company1 = Company()
            session.store(company1, "companies/")

            company2 = Company()
            session.store(company2, "companies|")

            session.save_changes()

            self.assertTrue(company1.Id.startswith("companies/000000000"))
            self.assertEqual("companies/3", company2.Id)

        with self.store.open_session(session_options=session_options) as session:
            company = session.advanced.cluster_transaction.get_compare_exchange_value("company:", Company)
            company.value.name = "HR"

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company|", Company())

            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company| cannot end with '|' or '/' as part of cluster transaction",
            )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("company/", Company())
            self.assertRaisesWithMessage(
                session.save_changes,
                RuntimeError,  # todo: change to RavenException when the Exception Dispatcher will be ready
                "Document id company/ cannot end with '|' or '/' as part of cluster transaction",
            )
