from typing import Optional

from ravendb.documents.indexes.index_creation import AbstractIndexCreationTask
from ravendb.documents.session.query_group_by import GroupByField
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class Article:
    def __init__(self, title: Optional[str] = None, description: Optional[str] = None, deleted: Optional[bool] = None):
        self.title = title
        self.description = description
        self.deleted = deleted


class ReduceResult:
    def __init__(self, count: Optional[int] = None, name: Optional[str] = None, age: Optional[int] = None):
        self.count = count
        self.name = name
        self.age = age


class UserProjection:
    def __init__(self, Id: str = None, name: str = None):
        self.Id = Id
        self.name = name


class TestQuery(TestBase):
    class UsersByName(AbstractIndexCreationTask):
        def __init__(self):
            super().__init__()
            self.map = "from c in docs.Users select new { c.name, count = 1 }"
            self.reduce = (
                "from result in results "
                "group result by result.name "
                "into g "
                "select new { name = g.Key, count = g.Sum(x => x.count) }"
            )

    class ReduceResult:
        def __init__(self, count: Optional[int] = None, name: Optional[str] = None, age: Optional[int] = None):
            self.count = count
            self.name = name
            self.age = age

    def __add_users(self):
        user1 = User(name="John", age=3)
        user2 = User(name="John", age=5)
        user3 = User(name="Tarzan", age=2)

        with self.store.open_session() as session:
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.store(user3, "users/3")
            session.save_changes()

        self.store.execute_index(TestQuery.UsersByName())
        self.wait_for_indexing(self.store)

    def setUp(self):
        super().setUp()

    def test_query_create_clauses_for_query_dynamically_when_the_query_empty(self):
        with self.store.open_session() as session:
            id1 = "users/1"
            id2 = "users/2"
            article1 = Article("foo", "bar", False)
            session.store(article1, id1)

            article2 = Article("foo", "bar", True)
            session.store(article2, id2)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.advanced.document_query(object_type=Article).and_also()

            self.assertEqual("from 'Articles'", query._to_string())
            query_result = list(query)
            self.assertEqual(2, len(query_result))

    def test_raw_query_skip_take(self):
        self.__add_users()
        with self.store.open_session() as session:
            users = list(session.advanced.raw_query("from users", User).skip(2).take(1))

            self.assertEqual(1, len(users))
            self.assertEqual("Tarzan", users[0].name)

    def test_query_map_reduce_index(self):
        self.__add_users()
        with self.store.open_session() as session:
            results = list(session.query_index("UsersByName", TestQuery.ReduceResult).order_by_descending("count"))

            self.assertEqual(2, results[0].count)
            self.assertEqual("John", results[0].name)

            self.assertEqual(1, results[1].count)
            self.assertEqual("Tarzan", results[1].name)

    def test_query_map_reduce_with_count(self):
        self.__add_users()
        with self.store.open_session() as session:
            results = list(
                session.query(object_type=User)
                .group_by("name")
                .select_key()
                .select_count()
                .order_by_descending("count")
                .of_type(ReduceResult)
            )

            self.assertEqual(2, results[0].count)
            self.assertEqual("John", results[0].name)

            self.assertEqual(1, results[1].count)
            self.assertEqual("Tarzan", results[1].name)

    def test_query_map_reduce_with_sum(self):
        self.__add_users()

        with self.store.open_session() as session:
            results = list(
                session.query(object_type=User)
                .group_by("name")
                .select_key()
                .select_sum(GroupByField("age"))
                .order_by_descending("age")
                .of_type(ReduceResult)
            )

            self.assertEqual(8, results[0].age)
            self.assertEqual("John", results[0].name)

            self.assertEqual(2, results[1].age)
            self.assertEqual("Tarzan", results[1].name)

    def test_query_with_projection(self):
        self.__add_users()

        with self.store.open_session() as session:
            projections = list(session.query(object_type=User).select_fields(UserProjection))
            self.assertEqual(3, len(projections))

            for projection in projections:
                self.assertIsNotNone(projection.Id)
                self.assertIsNotNone(projection.name)

    def test_query_with_projection_2(self):
        self.__add_users()

        with self.store.open_session() as session:
            projections = list(session.query(object_type=User).select_fields(UserProjection, "last_name", "Id"))
            self.assertEqual(3, len(projections))

            for projection in projections:
                self.assertIsNotNone(projection.Id)
                self.assertIsNone(projection.name)
