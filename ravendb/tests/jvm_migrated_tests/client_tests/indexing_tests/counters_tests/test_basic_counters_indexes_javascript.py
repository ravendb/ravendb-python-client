from ravendb.documents.indexes.counters import AbstractJavaScriptCountersIndexCreationTask
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class MyCounterIndex(AbstractJavaScriptCountersIndexCreationTask):
    def __init__(self):
        super(MyCounterIndex, self).__init__()
        self._maps = [
            "counters.map('Companies', 'HeartRate', function (counter) {\n return {\n heartBeat: counter.Value,\nname: counter.Name,\nuser: counter.DocumentId\n};\n})"
        ]


class MyMultiMapCounterIndex(AbstractJavaScriptCountersIndexCreationTask):
    class Result:
        def __init__(self, heart_beat: float = None, name: str = None, user: str = None):
            self.heart_beat = heart_beat
            self.name = name
            self.user = user

    def __init__(self):
        super(MyMultiMapCounterIndex, self).__init__()
        maps = set()
        maps.add(
            "counters.map('Companies', 'heartRate', function (counter) {\n"
            + "return {\n"
            + "    heartBeat: counter.Value,\n"
            + "    name: counter.Name,\n"
            + "    user: counter.DocumentId\n"
            + "};\n"
            + "})"
        )

        maps.add(
            "counters.map('Companies', 'heartRate2', function (counter) {\n"
            + "return {\n"
            + "    heartBeat: counter.Value,\n"
            + "    name: counter.Name,\n"
            + "    user: counter.DocumentId\n"
            + "};\n"
            + "})"
        )

        maps.add(
            "counters.map('Users', 'heartRate', function (counter) {\n"
            + "return {\n"
            + "    heartBeat: counter.Value,\n"
            + "    name: counter.Name,\n"
            + "    user: counter.DocumentId\n"
            + "};\n"
            + "})"
        )

        self.maps = maps


class TestBasicCountersIndexes_JavaScript(TestBase):
    def setUp(self):
        super(TestBasicCountersIndexes_JavaScript, self).setUp()

    def test_basic_multi_map_index(self):
        time_series_index = MyMultiMapCounterIndex()
        time_series_index.execute(self.store)

        with self.store.open_session() as session:
            company = Company()
            session.store(company)

            session.counters_for_entity(company).increment("heartRate", 3)
            session.counters_for_entity(company).increment("heartRate2", 5)

            user = User()
            session.store(user)
            session.counters_for_entity(user).increment("heartRate", 2)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(session.query_index_type(MyMultiMapCounterIndex, MyMultiMapCounterIndex.Result))
            self.assertEqual(3, len(results))
