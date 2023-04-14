from ravendb.documents.indexes.counters import AbstractCountersIndexCreationTask
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class Result:
    def __init__(self, heart_beat: str = None, name: str = None, user: str = None):
        self.heart_beat = heart_beat
        self.name = name
        self.user = user


class MyCounterIndex(AbstractCountersIndexCreationTask):
    def __init__(self):
        super(MyCounterIndex, self).__init__()
        self.map = (
            "counters.Companies.HeartRate.Select(counter => new {\n"
            "    heartBeat = counter.Value,\n"
            "    name = counter.Name,\n"
            "    user = counter.DocumentId\n"
            "})"
        )


class TestBasicCountersIndexes_StrongSyntax(TestBase):
    def setUp(self):
        super(TestBasicCountersIndexes_StrongSyntax, self).setUp()

    def test_basic_map_index(self):
        with self.store.open_session() as session:
            company = Company()
            session.store(company, "companies/1")
            session.counters_for_entity(company).increment("HeartRate", 7)
            session.save_changes()

        my_counter_index = MyCounterIndex()
        my_counter_index.execute(self.store)

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(session.query_index_type(MyCounterIndex, Result))
            self.assertEqual(1, len(results))

            result = results[0]

            self.assertEqual(7, result.heart_beat)
            self.assertEqual("companies/1", result.user)
            self.assertEqual("HeartRate", result.name)
