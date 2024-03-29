import atexit
import datetime
import threading
import time
import unittest
import sys
import os
from enum import Enum
from typing import Iterable, List, Optional, Set
from datetime import timedelta
from ravendb_embedded.embedded_server import EmbeddedServer

from ravendb.documents.operations.revisions import (
    ConfigureRevisionsOperationResult,
    RevisionsConfiguration,
    RevisionsCollectionConfiguration,
    ConfigureRevisionsOperation,
)
from ravendb.primitives import constants
from ravendb.documents.operations.indexes import GetIndexErrorsOperation
from ravendb.exceptions.exceptions import DatabaseDoesNotExistException
from ravendb.documents.indexes.definitions import IndexState, IndexErrors
from ravendb.documents.operations.statistics import GetStatisticsOperation
from ravendb.documents.store.definition import DocumentStore
from ravendb.exceptions.cluster import NoLoaderException
from ravendb.serverwide.database_record import DatabaseRecord
from ravendb.serverwide.operations.common import (
    CreateDatabaseOperation,
    DeleteDatabaseOperation,
    GetDatabaseRecordOperation,
)
from ravendb.tests.driver.raven_server_locator import RavenServerLocator
from ravendb.tests.driver.raven_test_driver import RavenTestDriver
from ravendb.tools.utils import Stopwatch

sys.path.append(os.path.abspath(__file__ + "/../../"))


class CompanyType(Enum):
    public = "public"
    private = "private"

    def __str__(self):
        return self.name


class User(object):
    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age


class UserWithId(User):
    def __init__(self, name=None, age=None, Id=None):
        super(UserWithId, self).__init__(name, age)
        self.Id = Id


class Dog(object):
    def __init__(self, name, owner):
        self.name = name
        self.owner = owner


class Address(object):
    def __init__(self, Id: str = None, country: str = None, city: str = None, street: str = None, zip_code: int = None):
        self.Id = Id
        self.country = country
        self.city = city
        self.street = street
        self.zip_code = zip_code


class Contact(object):
    def __init__(self, Id: str = None, first_name: str = None, surname: str = None, email: str = None):
        self.Id = Id
        self.first_name = first_name
        self.surname = surname
        self.email = email


class Order(object):
    def __init__(
        self,
        Id: str = None,
        company: str = None,
        employee: str = None,
        ordered_at: datetime.datetime = None,
        require_at: datetime.datetime = None,
        shipped_at: datetime.datetime = None,
        ship_to: Address = None,
        ship_via: str = None,
        freight: float = None,
        lines: Iterable = None,
    ):
        self.Id = Id
        self.company = company
        self.employee = employee
        self.ordered_at = ordered_at
        self.require_at = require_at
        self.shipped_at = shipped_at
        self.ship_to = ship_to
        self.ship_via = ship_via
        self.freight = freight
        self.lines = lines
        pass


class Company(object):
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        desc: str = None,
        email: str = None,
        address1: str = None,
        address2: str = None,
        address3: str = None,
        contacts: List[Contact] = None,
        phone: int = None,
        company_type: CompanyType = None,
        employees_ids: List[str] = None,
    ):
        self.Id = Id
        self.name = name
        self.desc = desc
        self.email = email
        self.address1 = address1
        self.address2 = address2
        self.address3 = address3
        self.contacts = contacts
        self.phone = phone
        self.company_type = company_type
        self.employees_ids = employees_ids


class Employee(object):
    def __init__(self, Id: str = None, first_name: str = None, last_name: str = None):
        self.Id = Id
        self.first_name = first_name
        self.last_name = last_name


class Patch(object):
    def __init__(self, patched):
        self.patched = patched


class TestBase(unittest.TestCase, RavenTestDriver):
    _global_embedded_server: Optional[EmbeddedServer] = None
    _global_secured_embedded_server: Optional[EmbeddedServer] = None

    _global_server_store: Optional[DocumentStore] = None
    _global_secured_server_store: Optional[DocumentStore] = None

    __run_server_lock = threading.Lock()

    index = 0

    class TestServiceLocator(RavenServerLocator):
        def get_server_path(self) -> str:
            return super().get_server_path()

        @property
        def server_path(self) -> str:
            return super().get_server_path()

        @property
        def command_arguments(self) -> List[str]:
            return [
                "--ServerUrl=http://127.0.0.1:0",
                "--ServerUrl.Tcp=tcp://127.0.0.1:38881",
                "--Features.Availability=Experimental",
            ]

    class TestSecuredServiceLocator(RavenServerLocator):
        ENV_CLIENT_CERTIFICATE_PATH = "RAVENDB_PYTHON_TEST_CLIENT_CERTIFICATE_PATH"
        ENV_SERVER_CERTIFICATE_PATH = "RAVENDB_PYTHON_TEST_SERVER_CERTIFICATE_PATH"
        ENV_TEST_CA_PATH = "RAVENDB_PYTHON_TEST_CA_PATH"
        ENV_HTTPS_SERVER_URL = "RAVENDB_PYTHON_TEST_HTTPS_SERVER_URL"

        def get_server_path(self) -> str:
            return super().get_server_path()

        @property
        def command_arguments(self) -> List[str]:
            https_server_url = self.https_server_url
            tcp_server_url = https_server_url.replace("https", "tcp", 1).rsplit(":", 1)[0] + ":38882"
            return [
                f"--Security.Certificate.Path={self.server_certificate_path}",
                f"--ServerUrl={https_server_url}",
                f"--ServerUrl.Tcp={tcp_server_url}",
            ]

        @property
        def https_server_url(self) -> str:
            https_server_url = os.environ[self.ENV_HTTPS_SERVER_URL]
            if https_server_url.isspace():
                raise ValueError(
                    "Unable to find RavenDB https server url. "
                    f"Please make sure {self.ENV_HTTPS_SERVER_URL} environment variable is set and is valid "
                    f"(current value = {https_server_url})"
                )
            return https_server_url

        @property
        def client_certificate_path(self) -> str:
            certificate_path = os.getenv(self.ENV_CLIENT_CERTIFICATE_PATH)
            if certificate_path.isspace():
                raise ValueError(
                    "Unable to find RavenDB server certificate path. "
                    f"Please make sure {self.ENV_CLIENT_CERTIFICATE_PATH} environment variable is set and is valid "
                    + f"(current value = {certificate_path})"
                )
            return certificate_path

        @property
        def server_certificate_path(self) -> str:
            certificate_path = os.getenv(self.ENV_SERVER_CERTIFICATE_PATH)
            if certificate_path.isspace():
                raise ValueError(
                    "Unable to find RavenDB server certificate path. "
                    f"Please make sure {self.ENV_SERVER_CERTIFICATE_PATH} environment variable is set and is valid "
                    + f"(current value = {certificate_path})"
                )
            return certificate_path

        @property
        def server_ca_path(self) -> str:
            return os.getenv(self.ENV_TEST_CA_PATH)

    def _get_locator(self, secured: bool):
        return self._secured_locator if secured else self._locator

    def _get_global_server_store(self, secured: bool):
        return self._global_secured_server_store if secured else self._global_server_store

    def _run_embedded_server(self, secured: bool):
        store, embedded_server = self._run_embedded_server_internal(self._get_locator(secured))

        if secured:
            TestBase._global_secured_server_store = store
            TestBase._global_secured_embedded_server = embedded_server
        else:
            TestBase._global_server_store = store
            TestBase._global_embedded_server = embedded_server

        atexit.register(threading.Thread(target=self._discard_embedded_server, args=[secured]).run)
        return store

    def _customize_db_record(self, db_record: DatabaseRecord) -> None:
        pass

    def _customize_store(self, store: DocumentStore) -> None:
        pass

    @property
    def secured_document_store(self) -> DocumentStore:
        if self._secured_store:
            return self._secured_store
        self._secured_store = self.get_document_store("test_db", True)
        return self._secured_store

    @property
    def test_client_certificate_url(self) -> str:
        return self._secured_locator.client_certificate_path

    @property
    def test_ca_certificate_url(self) -> str:
        return self._secured_locator.server_ca_path

    def get_document_store(
        self,
        database: Optional[str] = "test_db",
        secured: Optional[bool] = False,
        wait_for_indexing_timeout: Optional[datetime.timedelta] = None,
    ) -> DocumentStore:
        TestBase.index += 1
        name = f"{database}_{TestBase.index}"
        TestBase._report_info(f"get_document_store for db {database}.")

        if self._get_global_server_store(secured) is None:
            with self.__run_server_lock:
                if self._get_global_server_store(secured) is None:
                    self._run_embedded_server(secured)

        document_store = self._get_global_server_store(secured)
        database_record = DatabaseRecord(name)

        self._customize_db_record(database_record)

        document_store.maintenance.server.send(CreateDatabaseOperation(database_record))
        store = DocumentStore(document_store.urls, name)

        if secured:
            store.certificate_pem_path = self.test_client_certificate_url
            store.trust_store_path = self.test_ca_certificate_url

        self._customize_store(store)

        # todo: hook_leaked_connection_check
        store.initialize()

        def __after_close():
            if store not in self._document_stores:
                return

            try:
                store.maintenance.server.send(DeleteDatabaseOperation(store.database, True))
            except (DatabaseDoesNotExistException, NoLoaderException):
                pass

        store.add_after_close(__after_close)
        self._setup_database(store)

        if wait_for_indexing_timeout is not None:
            self.wait_for_indexing(store, name, wait_for_indexing_timeout)

        self._document_stores.add(store)
        return store

    @classmethod
    def _discard_embedded_server(cls, secured: bool) -> None:
        if secured:
            cls._global_secured_server_store.close()
            cls._global_secured_server_store = None
            cls._global_secured_embedded_server.close()
            cls._global_secured_embedded_server = None
        else:
            cls._global_server_store.close()
            cls._global_server_store = None
            cls._global_embedded_server.close()
            cls._global_embedded_server = None

    @staticmethod
    def delete_all_topology_files():
        import os

        file_list = [f for f in os.listdir(".") if f.endswith("topology")]
        for f in file_list:
            os.remove(f)

    @staticmethod
    def wait_for_database_topology(store, database_name, replication_factor=1):
        topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name)).topology
        while topology is not None and len(topology["Members"]) < replication_factor:
            topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name)).topology
        return topology

    @staticmethod
    def wait_for_indexing(
        store: DocumentStore, database: str = None, timeout: timedelta = timedelta(minutes=1), node_tag: str = None
    ):
        admin = store.maintenance.for_database(database)
        timestamp = datetime.datetime.now()
        while datetime.datetime.now() - timestamp < timeout:
            database_statistics = admin.send(GetStatisticsOperation("wait-for-indexing", node_tag))
            indexes = list(filter(lambda index: index.state != str(IndexState.DISABLED), database_statistics.indexes))
            if all(
                [
                    not index.stale
                    and not index.name.startswith(constants.Documents.Indexing.SIDE_BY_SIDE_INDEX_NAME_PREFIX)
                    for index in indexes
                ]
            ):
                return
            if any([IndexState.ERROR == index.state for index in indexes]):
                break
            try:
                time.sleep(0.1)
            except RuntimeError as e:
                raise RuntimeError(e)

        errors = admin.send(GetIndexErrorsOperation())  # admin.send(GetIndexErrorsOperation())
        all_index_errors_text = ""

        def __format_index_errors(errors_list: IndexErrors):
            errors_list_text = os.linesep.join(list(map(lambda error: f"-{error}", errors_list.errors)))
            return f"Index {errors_list.name} ({len(errors_list.errors)} errors): {os.linesep} {errors_list_text}"

        if errors is not None and len(errors) > 0:
            all_index_errors_text = os.linesep.join(list(map(__format_index_errors, errors)))

        raise TimeoutError(f"The indexes stayed stale for more than {timeout}. {all_index_errors_text}")

    @staticmethod
    def wait_for_indexing_errors(store: DocumentStore, timeout: timedelta, *index_names: str):
        sw = Stopwatch.create_started()

        while sw.elapsed() < timeout:
            indexes = store.maintenance.send(GetIndexErrorsOperation(*index_names))

            for index in indexes:
                if index.errors:
                    return indexes

            time.sleep(0.032)

        raise TimeoutError(f"Got no index error for more than {timeout.total_seconds()} seconds")

    def setConvention(self, conventions):
        self.conventions = conventions

    def setUp(self):
        RavenTestDriver.__init__(self)
        self._locator = TestBase.TestServiceLocator()
        self._secured_locator = TestBase.TestSecuredServiceLocator()
        self._document_stores: Set[DocumentStore] = set()
        conventions = getattr(self, "conventions", None)
        self.default_urls = ["http://127.0.0.1:8080"]
        self.default_database = "NorthWindTest"
        self.store = self.get_document_store(self.default_database)
        self._secured_store: Optional[DocumentStore] = None
        if conventions:
            self.store.conventions = conventions
        self.store.initialize()

        TestBase.wait_for_database_topology(self.store, self.store.database)
        self.index_map = 'from doc in docs select new{Tag = doc["@metadata"]["@collection"]}'

    def tearDown(self):
        if self.disposed:
            return

        exceptions = []

        for document_store in self._document_stores:
            try:
                document_store.close()
            except Exception as e:
                exceptions.append(e)

        self._disposed = True

        if exceptions:
            raise RuntimeError(", ".join(list(map(str, exceptions))))

        TestBase.delete_all_topology_files()

    def assertRaisesWithMessage(self, func, exception, msg, *args, **kwargs):
        e = None
        try:
            func(*args, **kwargs)
        except Exception as ex:
            if not isinstance(ex, exception):
                raise AssertionError(f"Expected exception '{exception}' but got '{type(ex)}'.")
            e = ex
        self.assertIsNotNone(e)
        self.assertEqual(msg, e.args[0])

    def assertRaisesWithMessageContaining(self, func, exception, msg, *args, **kwargs):
        e = None
        try:
            func(*args, **kwargs)
        except Exception as ex:
            if not isinstance(ex, exception):
                raise AssertionError(f"Expected exception '{exception}' but got '{type(ex)}'.")
            e = ex
        self.assertIsNotNone(e)
        try:
            self.assertIn(msg, e.args[0])
        except AssertionError as ex:
            self.assertIn(msg, ex.args[0])

    def assertSequenceContainsElements(self, sequence, *args):
        for arg in args:
            self.assertIn(arg, sequence)

    @staticmethod
    def setup_revisions(
        store: DocumentStore, purge_on_delete: bool = None, minimum_revisions_to_keep: int = None
    ) -> ConfigureRevisionsOperationResult:
        revisions_configuration = RevisionsConfiguration()
        default_collection = RevisionsCollectionConfiguration()
        default_collection.purge_on_delete = purge_on_delete
        default_collection.minimum_revisions_to_keep = minimum_revisions_to_keep

        revisions_configuration.default_config = default_collection
        operation = ConfigureRevisionsOperation(revisions_configuration)

        return store.maintenance.send(operation)
