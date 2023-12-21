from typing import List

from ravendb import IndexFieldOptions
from ravendb.primitives import constants
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractJavaScriptIndexCreationTask
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class Product:
    def __init__(self, name: str, available: bool):
        self.name = name
        self.available = available


class Fanout:
    def __init__(self, foo: str = None, numbers: List[int] = None):
        self.foo = foo
        self.numbers = numbers


class FanoutByNumberWithReduce(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(FanoutByNumberWithReduce, self).__init__()
        self.maps = {
            "map('Fanouts', function (f){\n"
            + "                                var result = [];\n"
            + "                                for(var i = 0; i < f.numbers.length; i++)\n"
            + "                                {\n"
            + "                                    result.push({\n"
            + "                                        foo: f.foo,\n"
            + "                                        sum: f.numbers[i]\n"
            + "                                    });\n"
            + "                                }\n"
            + "                                return result;\n"
            + "                                })"
        }

        self.reduce = "groupBy(f => f.foo).aggregate(g => ({  foo: g.key, sum: g.values.reduce((total, val) => val.sum + total,0) }))"

    class Result:
        def __init__(self, foo: str = None, sum: int = None):
            self.foo = foo
            self.sum = sum


class UsersAndProductsByName(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(UsersAndProductsByName, self).__init__()
        self.maps = {
            "map('Users', function (u){ return { name: u.name, count: 1};})",
            "map('Products', function (p){ return { name: p.name, count: 1};})",
        }


class UsersByNameAndAnalyzedName(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(UsersByNameAndAnalyzedName, self).__init__()
        self.maps = {
            "map('Users', function (u){\n"
            + "                                    return {\n"
            + "                                        name: u.name,\n"
            + "                                        _: {$value: u.name, $name:'analyzed_name', $options: { indexing: 'Search', storage: true}}\n"
            + "                                    };\n"
            + "                                })"
        }

        field_options = {}
        self.fields = field_options

        index_field_options = IndexFieldOptions()
        index_field_options.indexing = FieldIndexing.SEARCH
        index_field_options.analyzer = "StandardAnalyzer"
        field_options[constants.Documents.Indexing.Fields.ALL_FIELDS] = index_field_options

    class Result:
        def __init__(self, analyzed_name: str):
            self.analyzed_name = analyzed_name


class UsersAndProductsByNameAndCount(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(UsersAndProductsByNameAndCount, self).__init__()
        self.maps = {
            "map('Users', function (u){ return { name: u.name, count: 1};})",
            "map('Products', function (p){ return { name: p.name, count: 1};})",
        }
        self.reduce = (
            "groupBy( x =>  x.name )\n"
            "                                .aggregate(g => {return {\n"
            "                                    name: g.key,\n"
            "                                    count: g.values.reduce((total, val) => val.count + total,0)\n"
            "                               };})"
        )


class UsersByName(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(UsersByName, self).__init__()
        self.maps = {"map('Users', function (u) { return { name: u.name, count: 1 } })"}


class UsersByNameWithAdditionalSources(AbstractJavaScriptIndexCreationTask):
    def __init__(self):
        super(UsersByNameWithAdditionalSources, self).__init__()
        self.maps = {"map('Users', function(u) { return { name: mr(u.name)}; })"}
        additional_sources = dict()
        additional_sources.update({"The Script": "function mr(x) { return 'Mr. ' + x; }"})
        self.additional_sources = additional_sources


class TestJavaScriptIndex(TestBase):
    def setUp(self):
        super(TestJavaScriptIndex, self).setUp()

    def test_can_index_map_reduce_with_fanout(self):
        self.store.execute_index(FanoutByNumberWithReduce())
        with self.store.open_session() as session:
            fanout_1 = Fanout("Foo", [4, 6, 11, 9])
            fanout_2 = Fanout("Bar", [3, 8, 5, 17])

            session.store(fanout_1)
            session.store(fanout_2)
            session.save_changes()

            self.wait_for_indexing(self.store)

            session.query_index("FanoutByNumberWithReduce", FanoutByNumberWithReduce.Result).where_equals(
                "sum", 33
            ).single()

    def test_can_use_java_script_multi_map_index(self):
        self.store.execute_index(UsersAndProductsByName())

        with self.store.open_session() as session:
            user = User(name="Brendan Eich")
            session.store(user)

            product = Product("Shampoo", True)
            session.store(product)

            session.save_changes()

            self.wait_for_indexing(self.store)

            session.query_index("UsersAndProductsByName", User).where_equals("name", "Brendan Eich").single()

    def test_can_use_java_script_index_with_dynamic_fields(self):
        self.store.execute_index(UsersByNameAndAnalyzedName())

        with self.store.open_session() as session:
            user = User(name="Brendan Eich")
            session.store(user)
            session.save_changes()

            self.wait_for_indexing(self.store)

            session.query_index("UsersByNameAndAnalyzedName", User).select_fields(
                UsersByNameAndAnalyzedName.Result
            ).search("analyzed_name", "Brendan").single()

    def test_can_use_java_script_map_reduce_index(self):
        self.store.execute_index(UsersAndProductsByNameAndCount())

        with self.store.open_session() as session:
            user = User(name="Brendan Eich")
            session.store(user)

            product = Product("Shampoo", True)
            session.store(product)

            session.save_changes()

            self.wait_for_indexing(self.store)
            session.query_index("UsersAndProductsByNameAndCount").where_equals("name", "Brendan Eich").single()

    def test_can_use_java_script_index(self):
        self.store.execute_index(UsersByName())

        with self.store.open_session() as session:
            user = User(name="Brendan Eich")

            session.store(user)
            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            single = session.query_index("UsersByName", User).where_equals("name", "Brendan Eich").single()

            self.assertIsNotNone(single)

    def test_can_use_java_script_index_with_additional_sources(self):
        self.store.execute_index(UsersByNameWithAdditionalSources())

        with self.store.open_session() as session:
            user = User(name="Brendan Eich")
            session.store(user)
            session.save_changes()

            self.wait_for_indexing(self.store)

            session.query_index("UsersByNameWithAdditionalSources", User).where_equals(
                "name", "Mr. Brendan Eich"
            ).single()
