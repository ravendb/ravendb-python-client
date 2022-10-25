from typing import List

from ravendb.documents.indexes.index_creation import AbstractJavaScriptIndexCreationTask
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
