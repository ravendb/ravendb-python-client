from ravendb import GetTermsOperation
from ravendb.documents.indexes.counters import AbstractJavaScriptCountersIndexCreationTask
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company, Address
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


class AverageHeartRate_WithLoad(AbstractJavaScriptCountersIndexCreationTask):
    class Result:
        def __init__(self, heart_beat: float = None, city: str = None, count: int = None):
            self.heart_beat = heart_beat
            self.city = city
            self.count = count

    def __init__(self):
        super(AverageHeartRate_WithLoad, self).__init__()
        mapp = (
            "counters.map('Users', 'heartRate', function (counter) {\n"
            "var user = load(counter.DocumentId, 'Users');\n"
            "var address = load(user.address_id, 'Addresses');\n"
            "return {\n"
            "    heartBeat: counter.Value,\n"
            "    count: 1,\n"
            "    city: address.city\n"
            "};\n"
            "})"
        )
        self.maps = [mapp]

        self.reduce = (
            "groupBy(r => ({ city: r.city }))\n"
            " .aggregate(g => ({\n"
            "     heartBeat: g.values.reduce((total, val) => val.heartBeat + total, 0) / g.values.reduce((total, val) => val.count + total, 0),\n"
            "     city: g.key.city,\n"
            "     count: g.values.reduce((total, val) => val.count + total, 0)\n"
            " }))"
        )


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

    def test_basic_map_reduce_index_with_load(self):
        with self.store.open_session() as session:
            for i in range(10):
                address = Address(city="NY")
                session.store(address, f"addresses/{i}")

                user = User(address_id=f"addresses/{i}")
                session.store(user, f"users/{i}")

                session.counters_for_entity(user).increment("heartRate", 180 + i)

            session.save_changes()

        time_series_index = AverageHeartRate_WithLoad()
        index_name = time_series_index.index_name
        index_definition = time_series_index.create_index_definition()

        time_series_index.execute(self.store)

        self.wait_for_indexing(self.store)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "heartBeat", None))
        self.assertEqual(1, len(terms))
        self.assertIn("184.5", terms)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "count", None))
        self.assertEqual(1, len(terms))
        self.assertIn("10", terms)

        terms = self.store.maintenance.send(GetTermsOperation(index_name, "city", None))
        self.assertEqual(1, len(terms))
        self.assertIn("ny", terms)
