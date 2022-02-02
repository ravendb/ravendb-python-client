from pyravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
    CompareExchangeResult,
    DeleteCompareExchangeValueOperation,
)
from pyravendb.documents.operations.statistics import DetailedDatabaseStatistics, GetDetailedStatisticsOperation
from pyravendb.tests.test_base import TestBase, UserWithId


class Person:
    def __init__(self, Id: str = None, name: str = None, address_id: str = None):
        self.Id = Id
        self.name = name
        self.address_id = address_id


class TestRavenDB10038(TestBase):
    def setUp(self):
        super().setUp()

    def test_compare_exchange_and_identities_count(self):
        stats: DetailedDatabaseStatistics = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(0, stats.count_of_identities)
        self.assertEqual(0, stats.count_of_compare_exchange)

        with self.store.open_session() as session:
            person = Person("people|")
            session.store(person)
            session.save_changes()

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(1, stats.count_of_identities)
        self.assertEqual(0, stats.count_of_compare_exchange)

        with self.store.open_session() as session:
            person = Person("people|")
            session.store(person)

            user = UserWithId(Id="users|")
            session.store(user)

            session.save_changes()

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(2, stats.count_of_identities)
        self.assertEqual(0, stats.count_of_compare_exchange)

        self.store.operations.send(PutCompareExchangeValueOperation("key/1", Person(), 0))

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(2, stats.count_of_identities)
        self.assertEqual(1, stats.count_of_compare_exchange)

        result: CompareExchangeResult[Person] = self.store.operations.send(
            PutCompareExchangeValueOperation("key/2", Person(), 0)
        )
        self.assertTrue(result.successful)

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(2, stats.count_of_identities)
        self.assertEqual(2, stats.count_of_compare_exchange)

        result = self.store.operations.send(PutCompareExchangeValueOperation("key/2", Person(), result.index))
        self.assertTrue(result.successful)

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(2, stats.count_of_identities)
        self.assertEqual(2, stats.count_of_compare_exchange)

        result = self.store.operations.send(DeleteCompareExchangeValueOperation(Person, "key/2", result.index))
        self.assertTrue(result.successful)

        stats = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(2, stats.count_of_identities)
        self.assertEqual(1, stats.count_of_compare_exchange)
