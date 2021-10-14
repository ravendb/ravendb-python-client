import unittest

from pyravendb.raven_operations.server_operations import (
    DeleteDatabaseOperation,
    CreateDatabaseOperation,
)
from pyravendb.store.document_store import DocumentStore
from pyravendb.tests.jvm_migrated_tests.test_load import UserWithId
from pyravendb.tests.test_base import TestBase


class CertTest(unittest.TestCase):
    def setUp(self):
        self.default_urls = [
            "https://a.test.gracjan-t3st.cloudtest.ravendb.org",
            "https://b.test.gracjan-t3st.cloudtest.ravendb.org",
            "https://c.test.gracjan-t3st.cloudtest.ravendb.org",
        ]
        self.default_database = "NorthWindTest"
        self.store = DocumentStore(
            urls=self.default_urls,
            database=self.default_database,
            certificate={
                "pfx": "C:\\rvdbcl\\3node\\test.gracjan-t3st.client.certificate.with.password.pfx",
                "password": "6D238B4C3541D11A6CDF84EE14E392F",
            },
        )
        self.store.initialize()

    def tearDown(self):
        self.store.close()

    def test_cert_cloud(self):
        with self.store.open_session() as session:
            st = UserWithId("Stefan", 20)
            session.store(st)
            session.save_changes()
            session.delete(st)
            session.save_changes()
