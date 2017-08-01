from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from datetime import datetime, timedelta
from pyravendb.tools.utils import Utils


class TestOperations(TestBase):
    def setUpClass(cls):
        super(TestOperations, cls).setUpClass()
