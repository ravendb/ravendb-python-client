import datetime
import unittest

from ravendb.documents.indexes.definitions import IndexDefinition
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.documents.operations.statistics import GetCollectionStatisticsOperation
from ravendb.documents.queries.misc import SearchOperator
from ravendb.tests.test_base import TestBase, User, UserWithId, Order
from ravendb.documents.session.misc import OrderingType
from ravendb.tools.utils import Utils


class UserProjection:
    def __init__(self, name=None, key=None):
        self.Id = key
        self.name = name


class UserFull(UserWithId):
    def __init__(self, last_name=None, address_id=None, count=None):
        super(UserFull, self).__init__()
        self.last_name = last_name
        self.address_id = address_id
        self.count = count


class UsersByName(IndexDefinition):
    def __init__(self, name="region"):
        maps = ["from c in docs.Users select new " " {" "    c.name, " "    count = 1" "}"]

        reduce = (
            "from result in results "
            "group result by result.name "
            "into g "
            "select new "
            "{ "
            "  name = g.Key, "
            "  count = g.Sum(x => x.count) "
            "}"
        )

        super(UsersByName, self).__init__(reduce=reduce, name=name)
        self.maps = maps


class OrderTime(IndexDefinition):
    def __init__(self, name="OrderTime"):
        maps = ["from order in docs.Orders select new {delay = order.shipped_at - ((DateTime?)order.ordered_at)}"]
        super(OrderTime, self).__init__(name=name)
        self.maps = maps


class OrderTimeResult:
    def __init__(self, delay):
        self.delay = delay


class Dog:
    def __init__(self, Id, name="Reksio", breed="Shepherd", color="White", age=1, is_vaccinated=True):
        self.Id = Id
        self.name = name
        self.breed = breed
        self.color = color
        self.age = age
        self.is_vaccinated = is_vaccinated


class DogsIndexResult:
    def __init__(self, name, age, is_vaccinated):
        self.name = name
        self.age = age
        self.is_vaccinated = is_vaccinated


class DogsIndex(IndexDefinition):
    def __init__(self, name="doggos"):
        maps = ["from dog in docs.dogs select new { dog.name, dog.age, dog.is_vaccinated }"]
        super(DogsIndex, self).__init__(name=name)
        self.maps = maps


class TestQuery(TestBase):
    def setUp(self):
        super(TestQuery, self).setUp()

    def add_users(self, user_class=UserWithId, args1=None, args2=None, args3=None, **kwargs):
        if args3 is None:
            args3 = []
        if args2 is None:
            args2 = []
        if args1 is None:
            args1 = []
        user1 = user_class("John", 3, args1)
        user2 = user_class("John", 5, args2)
        user3 = user_class("Tarzan", 2, args3)
        with self.store.open_session() as session:
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.store(user3, "users/3")
            session.save_changes()
        self.store.maintenance.send(PutIndexesOperation(UsersByName()))
        self.wait_for_indexing(self.store, self.store.database)

    def add_dogs(self, session):
        session.store(Dog("docs/1", "Snoopy", "Beagle", "White", 6, True))
        session.store(Dog("docs/2", "Brian", "Labrador", "White", 12, False))
        session.store(Dog("docs/3", "Django", "Jack Russel", "Black", 3, True))
        session.store(Dog("docs/4", "Beethoven", "St. Bernard", "Brown", 1, False))
        session.store(Dog("docs/5", "Scooby Doo", "Great Dane", "Brown", 0, False))
        session.store(Dog("docs/6", "Old Yeller", "Black Mouth Cur", "White", 2, True))
        session.store(Dog("docs/7", "Benji", "Mixed", "White", 0, False))
        session.store(Dog("docs/8", "Lassie", "Collie", "Brown", 6, True))

    def test_collection_stats(self):
        with self.store.open_session() as session:
            user1 = UserWithId("John")
            user2 = UserWithId("Jane")
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        stats = self.store.maintenance.send(GetCollectionStatisticsOperation())
        self.assertEqual(2, stats.count_of_documents)
        self.assertEqual(2, stats.collections["UserWithIds"])

    def test_query_simple(self):
        with self.store.open_session() as session:
            session.store(User("John"), "users/1")
            session.store(User("Jane"), "users/2")
            session.store(User("Tarzan"), "users/3")
            session.save_changes()
            query_result = list(session.query(object_type=User))
            self.assertEqual(3, len(query_result))

    def test_query_with_where_clause(self):
        with self.store.open_session() as session:
            session.store(User("John"), "users/1")
            session.store(User("Jane"), "users/2")
            session.store(User("Tarzan"), "users/3")
            session.save_changes()

            query_result1 = list(session.query(object_type=User).where_starts_with("name", "J"))
            query_result2 = list(session.query(object_type=User).where_equals("name", "Tarzan"))
            query_result3 = list(session.query(object_type=User).where_ends_with("name", "n"))

            self.assertEqual(2, len(query_result1))
            self.assertEqual(1, len(query_result2))
            self.assertEqual(2, len(query_result3))

    def test_query_with_where_in(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_in("name", ["Tarzan", "no-such"]))
            self.assertEqual(1, len(users))

    def test_query_with_where_between(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_between("age", 4, 5))
            self.assertEqual(1, len(users))
            self.assertEqual("John", users[0].name)

    def test_query_with_where_less_than(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_less_than("age", 3))
        self.assertEqual(1, len(users))
        self.assertEqual("Tarzan", users[0].name)

    def test_query_with_where_less_than_or_equal(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_less_than_or_equal("age", 3))
        self.assertEqual(2, len(users))

    def test_query_with_where_greater_than(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_greater_than("age", 3))
        self.assertEqual(1, len(users))
        self.assertEqual("John", users[0].name)

    def test_query_with_where_greater_than_or_equal(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_greater_than_or_equal("age", 3))
        self.assertEqual(2, len(users))

    def test_query_search_with_or(self):
        self.add_users()
        with self.store.open_session() as session:
            unique_names = list(session.query(object_type=UserWithId).search("name", "Tarzan John", SearchOperator.OR))
            self.assertEqual(3, len(unique_names))

    def test_query_no_tracking(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).no_tracking())
            self.assertEqual(3, len(users))
            for user in users:
                self.assertFalse(session.is_loaded_or_deleted(user.Id))

    def test_query_long_request(self):
        with self.store.open_session() as session:
            long_name = str("x") * 2048
            user = User(long_name)
            session.store(user, "users/1")
            session.save_changes()
            result = list(session.query_collection("Users", User).where_equals("name", long_name))
            self.assertEqual(1, len(result))

    def test_query_where_exact(self):
        self.add_users()
        with self.store.open_session() as session:
            self.assertEqual(1, len(list(session.query(object_type=UserWithId).where_equals("name", "tarzan"))))
            self.assertEqual(0, len(list(session.query(object_type=UserWithId).where_equals("name", "tarzan", True))))
            self.assertEqual(1, len(list(session.query(object_type=UserWithId).where_equals("name", "Tarzan", True))))

    def test_query_first(self):
        self.add_users()
        with self.store.open_session() as session:
            first = session.query(object_type=UserWithId).first()
            self.assertIsNotNone(first)
            self.assertIsNotNone(session.query(object_type=UserWithId).where_equals("name", "Tarzan").single())
            self.assertIsNotNone(first)
            with self.assertRaises(ValueError):
                session.query(object_type=UserWithId).single()

    def test_query_lucene(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).where_lucene("name", "Tarzan"))
            self.assertEqual(1, len(users))
            for user in users:
                self.assertEqual(user.name, "Tarzan")

    def test_query_where_not(self):
        self.add_users()
        with self.store.open_session() as session:
            self.assertEqual(2, len(list(session.query(object_type=UserWithId).not_().where_equals("name", "tarzan"))))
            self.assertEqual(2, len(list(session.query(object_type=UserWithId).where_not_equals("name", "tarzan"))))
            self.assertEqual(
                2, len(list(session.query(object_type=UserWithId).where_not_equals("name", "Tarzan", True)))
            )

    def test_query_skip_take(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(session.query(object_type=UserWithId).order_by("name").skip(2).take(1))
            self.assertEqual(1, len(users))
            self.assertEqual("Tarzan", users[0].name)

    def test_query_where_exists(self):
        self.add_users()
        with self.store.open_session() as session:
            self.assertEqual(3, len(list(session.query(object_type=UserWithId).where_exists("name"))))
            self.assertEqual(
                3,
                len(
                    list(
                        session.query(object_type=UserWithId)
                        .where_exists("name")
                        .and_also()
                        .not_()
                        .where_exists("no_such_field")
                    )
                ),
            )

    def test_query_random_order(self):
        self.add_users()
        with self.store.open_session() as session:
            self.assertEqual(3, len(list(session.query(object_type=UserWithId).random_ordering())))
            self.assertEqual(3, len(list(session.query(object_type=UserWithId).random_ordering("123"))))

    def test_query_with_boost(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(
                session.query(object_type=UserWithId)
                .where_equals("name", "Tarzan")
                .boost(5)
                .or_else()
                .where_equals("name", "John")
                .boost(2)
                .order_by_score()
            )
            self.assertEqual(3, len(users))
            names = list(map(lambda user: user.name, users))
            self.assertEqual(["Tarzan", "John", "John"], names)

            users = list(
                session.query(object_type=UserWithId)
                .where_equals("name", "Tarzan")
                .boost(2)
                .or_else()
                .where_equals("name", "John")
                .boost(5)
                .order_by_score()
            )
            self.assertEqual(3, len(users))
            names = list(map(lambda user: user.name, users))
            self.assertEqual(["John", "John", "Tarzan"], names)

    def test_query_parameters(self):
        self.add_users()
        with self.store.open_session() as session:
            x = list(
                session.advanced.raw_query("FROM UserWithIds WHERE name = $name", object_type=UserWithId).add_parameter(
                    "name", "Tarzan"
                )
            )
            self.assertEqual(1, len(x))

    def test_parameters_in_raw_query(self):
        self.add_users()
        with self.store.open_session() as session:
            users = list(
                session.advanced.raw_query("FROM UserWithIds WHERE age == $p0", UserWithId).add_parameter("p0", 5)
            )
            self.assertEqual(1, len(users))
            self.assertEqual("John", users[0].name)

    def test_query_by_index(self):
        index = DogsIndex()
        self.store.maintenance.send(PutIndexesOperation(index))
        with self.store.open_session() as session:
            self.add_dogs(session)
            session.save_changes()

        self.wait_for_indexing(self.store, self.store.database)

        with self.store.open_session() as session:
            query_result = list(
                session.query_index(index.name, Dog)
                .where_greater_than("age", 2)
                .and_also()
                .where_equals("is_vaccinated", False)
            )

            self.assertEqual(1, len(query_result))
            self.assertEqual("Brian", query_result[0].name)

            query_result_2 = list(
                session.query_index(index.name, Dog)
                .where_less_than_or_equal("age", 2)
                .and_also()
                .where_equals("is_vaccinated", False)
            )
            self.assertEqual(3, len(query_result_2))

            names = list(map(lambda dog: dog.name, query_result_2))
            self.assertSequenceContainsElements(names, "Beethoven", "Scooby Doo", "Benji")

    def test_query_with_duration(self):
        now = datetime.datetime.utcnow()
        index = OrderTime()
        self.store.maintenance.send(PutIndexesOperation(index))
        with self.store.open_session() as session:
            order1 = Order(company="hours", ordered_at=Utils.add_hours(now, -2), shipped_at=now)
            session.store(order1)
            order2 = Order(company="days", ordered_at=Utils.add_days(now, -2), shipped_at=now)
            session.store(order2)
            order3 = Order(company="minutes", ordered_at=Utils.add_minutes(now, -2), shipped_at=now)
            session.store(order3)
            session.save_changes()

        self.wait_for_indexing(self.store, self.store.database)

        with self.store.open_session() as session:
            delay1 = list(
                map(
                    lambda x: x.company,
                    list(session.query_index(index.name, Order).where_less_than("delay", datetime.timedelta(hours=3))),
                )
            )
            self.assertEqual(2, len(delay1))
            self.assertSequenceContainsElements(delay1, "hours", "minutes")

            delay2 = list(
                map(
                    lambda x: x.company,
                    list(
                        session.query_index(index.name, Order).where_greater_than("delay", datetime.timedelta(hours=3))
                    ),
                )
            )
            self.assertEqual(1, len(delay2))
            self.assertSequenceContainsElements(delay2, "days")
