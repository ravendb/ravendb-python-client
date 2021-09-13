import datetime
import time
import unittest
import sys
import os
from typing import Iterable
from datetime import timedelta
from pyravendb import constants
from pyravendb.data.indexes import IndexState, IndexErrors

sys.path.append(os.path.abspath(__file__ + "/../../"))

from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.server_operations import *
from pyravendb.raven_operations.maintenance_operations import (
    IndexDefinition,
    PutIndexesOperation,
    GetStatisticsOperation,
    GetIndexErrorsOperation,
)


class User(object):
    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age


class UserWithId(User):
    def __init__(self, name=None, age=None, identifier=None):
        super(UserWithId, self).__init__(name, age)
        self.Id = identifier


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


class OrderLine(object):
    def __init__(self, product: str, product_name: str, price_per_unit: float, quantity: int, discount: float):
        self.product = product
        self.product_name = product_name
        self.price_per_unit = price_per_unit
        self.quantity = quantity
        self.discount = discount


class Patch(object):
    def __init__(self, patched):
        self.patched = patched


class TestBase(unittest.TestCase):
    @staticmethod
    def delete_all_topology_files():
        import os

        file_list = [f for f in os.listdir(".") if f.endswith("topology")]
        for f in file_list:
            os.remove(f)

    @staticmethod
    def wait_for_database_topology(store, database_name, replication_factor=1):
        topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name))
        while topology is not None and len(topology["Members"]) < replication_factor:
            topology = store.maintenance.server.send(GetDatabaseRecordOperation(database_name))
        return topology

    @staticmethod
    def wait_for_indexing(
        store: DocumentStore, database: str = None, timeout: timedelta = timedelta(minutes=1), node_tag: str = None
    ):
        admin = store.maintenance.for_database(database)
        timestamp = datetime.datetime.now()
        while datetime.datetime.now() - timestamp < timeout:
            database_statistics = admin.send(GetStatisticsOperation("wait-for-indexing", node_tag))
            indexes = list(filter(lambda index: index["State"] != IndexState.disabled, database_statistics["Indexes"]))
            if all(
                [
                    not index["IsStale"]
                    and not index["Name"].startswith(constants.Documents.Indexing.SIDE_BY_SIDE_INDEX_NAME_PREFIX)
                    for index in indexes
                ]
            ):
                return
            if any([IndexState.error == index["State"] for index in indexes]):
                break
            try:
                time.sleep(0.1)
            except RuntimeError as e:
                raise RuntimeError(e)

        errors = admin.send(GetIndexErrorsOperation())
        all_index_errors_text = ""

        def __format_index_errors(errors_list: IndexErrors):
            errors_list_text = os.linesep.join(list(map(lambda error: f"-{error}", errors_list.errors)))
            return f"Index {errors_list.name} ({len(errors_list.errors)} errors): {os.linesep} {errors_list_text}"

        if errors is not None and len(errors) > 0:
            all_index_errors_text = os.linesep.join(list(map(__format_index_errors, errors)))

        raise TimeoutError(f"The indexes stayed stale for more than {timeout}. {all_index_errors_text}")

    def setConvention(self, conventions):
        self.conventions = conventions

    def setUp(self):
        conventions = getattr(self, "conventions", None)
        self.default_urls = ["http://127.0.0.1:8080"]
        self.default_database = "NorthWindTest"
        self.store = DocumentStore(urls=self.default_urls, database=self.default_database)
        if conventions:
            self.store.conventions = conventions
        self.store.initialize()
        created = False
        while not created:
            try:
                self.store.maintenance.server.send(CreateDatabaseOperation(database_name=self.default_database))
                created = True
            except Exception as e:
                if "already exists!" in str(e):
                    self.store.maintenance.server.send(
                        DeleteDatabaseOperation(database_name=self.default_database, hard_delete=True)
                    )
                    continue
                raise
        TestBase.wait_for_database_topology(self.store, self.default_database)

        self.index_map = 'from doc in docs select new{Tag = doc["@metadata"]["@collection"]}'
        self.store.maintenance.send(PutIndexesOperation(IndexDefinition("AllDocuments", maps=self.index_map)))

    def tearDown(self):
        self.store.maintenance.server.send(DeleteDatabaseOperation(database_name="NorthWindTest", hard_delete=True))
        self.store.close()

    def assertRaisesWithMessage(self, func, exception, msg, *args, **kwargs):
        e = None
        try:
            func(*args, **kwargs)
        except exception as ex:
            e = ex
        self.assertIsNotNone(e)
        self.assertEqual(msg, e.args[0])

    def assertSequenceContainsElements(self, sequence, *args):
        for arg in args:
            self.assertIn(arg, sequence)
