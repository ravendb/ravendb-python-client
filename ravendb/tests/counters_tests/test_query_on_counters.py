import unittest
from typing import Callable, Union, Dict

from ravendb.tests.test_base import TestBase, Order, Employee, Company, User


class TestQueryOnCounters(TestBase):
    def setUp(self):
        super(TestQueryOnCounters, self).setUp()

    def _counters_caching_should_handle_deletion(
        self, session_consumer: Callable, expected_counter_value: Union[None, int]
    ):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.counters_for("orders/1-A").increment("downloads", 100)
            session.counters_for("orders/1-A").increment("uploads", 123)
            session.counters_for("orders/1-A").increment("bugs", 0xDEAD)
            session.save_changes()

        with self.store.open_session() as session:
            list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads")))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(100, counter_value)

            with self.store.open_session() as write_session:
                write_session.counters_for("orders/1-A").increment("downloads", 200)
                write_session.save_changes()

            list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads")))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(300, counter_value)
            session.save_changes()

            with self.store.open_session() as write_session:
                write_session.counters_for("orders/1-A").delete("downloads")
                write_session.save_changes()

            session_consumer(session)
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(expected_counter_value, counter_value)

    def test_session_query_include_single_counter(self):
        with self.store.open_session() as session:
            order1 = Order(company="companies/1-A")
            session.store(order1, "orders/1-A")

            order2 = Order(company="companies/2-A")
            session.store(order2, "orders/2-A")

            order3 = Order(company="companies/3-A")
            session.store(order3, "orders/3-A")

            session.counters_for("orders/1-A").increment("downloads", 100)
            session.counters_for("orders/2-A").increment("downloads", 200)
            session.counters_for("orders/3-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(lambda x: x.include_counters("downloads"))
            self.assertEqual("from 'Orders' include counters('downloads')", query._to_string())
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests)

            order = query_result[0]
            self.assertEqual("orders/1-A", order.Id)

            val = session.counters_for(order).get("downloads")
            self.assertEqual(100, val)

            order = query_result[1]
            self.assertEqual("orders/2-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(200, val)

            order = query_result[2]
            self.assertEqual("orders/3-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(300, val)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_session_query_include_multiple_counters(self):
        with self.store.open_session() as session:
            order1 = Order(company="companies/1-A")
            session.store(order1, "orders/1-A")

            order2 = Order(company="companies/2-A")
            session.store(order2, "orders/2-A")

            order3 = Order(company="companies/3-A")
            session.store(order3, "orders/3-A")

            session.counters_for("orders/1-A").increment("downloads", 100)
            session.counters_for("orders/2-A").increment("downloads", 200)
            session.counters_for("orders/3-A").increment("downloads", 300)

            session.counters_for("orders/1-A").increment("likes", 1000)
            session.counters_for("orders/2-A").increment("likes", 2000)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(lambda x: x.include_counters("downloads", "likes"))
            self.assertIn(
                query._to_string(),
                [
                    "from 'Orders' include counters('downloads'),counters('likes')",
                    "from 'Orders' include counters('likes'),counters('downloads')",
                ],
            )
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests)

            order = query_result[0]
            self.assertEqual("orders/1-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(100, val)
            val = session.counters_for(order).get("likes")
            self.assertEqual(1000, val)

            order = query_result[1]
            self.assertEqual("orders/2-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(200, val)
            val = session.counters_for(order).get("likes")
            self.assertEqual(2000, val)

            order = query_result[2]
            self.assertEqual("orders/3-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(300, val)
            val = session.counters_for(order).get("likes")
            self.assertIsNone(val)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_session_query_include_counter_of_related_document(self):
        with self.store.open_session() as session:
            session.store(Order(employee="employees/1-A"), "orders/1-A")
            session.store(Order(employee="employees/2-A"), "orders/2-A")
            session.store(Order(employee="employees/3-A"), "orders/3-A")
            session.store(Employee(first_name="Aviv"), "employees/1-A")
            session.store(Employee(first_name="Jerry"), "employees/2-A")
            session.store(Employee(first_name="Bob"), "employees/3-A")

            session.counters_for("employees/1-A").increment("downloads", 100)
            session.counters_for("employees/2-A").increment("downloads", 200)
            session.counters_for("employees/3-A").increment("downloads", 300)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(lambda i: i.include_counters("downloads", path="employee"))
            self.assertEqual("from 'Orders' include counters(employee, 'downloads')", query._to_string())

    def test_session_query_include_counters_of_related_document(self):
        with self.store.open_session() as session:
            session.store(Order(employee="employees/1-A"), "orders/1-A")
            session.store(Order(employee="employees/2-A"), "orders/2-A")
            session.store(Order(employee="employees/3-A"), "orders/3-A")
            session.store(Employee(first_name="Aviv"), "employees/1-A")
            session.store(Employee(first_name="Jerry"), "employees/2-A")
            session.store(Employee(first_name="Bob"), "employees/3-A")
            session.counters_for("employees/1-A").increment("downloads", 100)
            session.counters_for("employees/2-A").increment("downloads", 200)
            session.counters_for("employees/3-A").increment("downloads", 300)
            session.counters_for("employees/1-A").increment("likes", 1000)
            session.counters_for("employees/2-A").increment("likes", 2000)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(
                lambda i: i.include_counters("downloads", "likes", path="employee")
            )

            self.assertIn(
                query._to_string(),
                [
                    "from 'Orders' include counters(employee, 'downloads'),counters(employee, 'likes')",
                    "from 'Orders' include counters(employee, 'likes'),counters(employee, 'downloads')",
                ],
            )

    def test_session_query_include_counters_of_document_and_of_related_document(self):
        with self.store.open_session() as session:
            session.store(Order(employee="employees/1-A"), "orders/1-A")
            session.store(Order(employee="employees/2-A"), "orders/2-A")
            session.store(Order(employee="employees/3-A"), "orders/3-A")
            session.store(Employee(first_name="Aviv"), "employees/1-A")
            session.store(Employee(first_name="Jerry"), "employees/2-A")
            session.store(Employee(first_name="Bob"), "employees/3-A")
            session.counters_for("orders/1-A").increment("likes", 100)
            session.counters_for("orders/2-A").increment("likes", 200)
            session.counters_for("orders/3-A").increment("likes", 300)
            session.counters_for("employees/1-A").increment("downloads", 1000)
            session.counters_for("employees/2-A").increment("downloads", 2000)
            session.counters_for("employees/3-A").increment("downloads", 3000)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(
                lambda i: i.include_counters("likes").include_counters("downloads", path="employee")
            )
            self.assertEqual(
                "from 'Orders' include counters('likes'),counters(employee, 'downloads')", query._to_string()
            )

            orders = list(query.order_by("employee"))
            self.assertEqual(1, session.advanced.number_of_requests)
            order = orders[0]
            self.assertEqual("orders/1-A", order.Id)
            val = session.counters_for(order).get("likes")
            self.assertEqual(100, val)
            order = orders[1]
            self.assertEqual("orders/2-A", order.Id)
            val = session.counters_for(order).get("likes")
            self.assertEqual(200, val)
            order = orders[2]
            self.assertEqual("orders/3-A", order.Id)
            val = session.counters_for(order).get("likes")
            self.assertEqual(300, val)

            val = session.counters_for("employees/1-A").get("downloads")
            self.assertEqual(1000, val)
            val = session.counters_for("employees/2-A").get("downloads")
            self.assertEqual(2000, val)
            val = session.counters_for("employees/3-A").get("downloads")
            self.assertEqual(3000, val)

    def test_session_query_include_all_documents(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.store(Order(company="companies/2-A"), "orders/2-A")
            session.store(Order(company="companies/3-A"), "orders/3-A")
            session.counters_for("orders/1-A").increment("likes", 1000)
            session.counters_for("orders/2-A").increment("likes", 2000)
            session.counters_for("orders/1-A").increment("downloads", 100)
            session.counters_for("orders/2-A").increment("downloads", 200)
            session.counters_for("orders/3-A").increment("downloads", 300)
            session.counters_for("orders/1-A").increment("votes", 10000)
            session.counters_for("orders/3-A").increment("cats", 5)
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(lambda i: i.include_all_counters())
            self.assertEqual("from 'Orders' include counters()", query._to_string())
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests)

            order = query_result[0]
            self.assertEqual("orders/1-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(3, len(dic))
            items = dic.items()

            self.assertIn(("downloads", 100), items)
            self.assertIn(("likes", 1000), items)
            self.assertIn(("votes", 10000), items)

            order = query_result[1]
            self.assertEqual("orders/2-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(2, len(dic))
            items = dic.items()

            self.assertIn(("downloads", 200), items)
            self.assertIn(("likes", 2000), items)

            order = query_result[2]
            self.assertEqual("orders/3-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(2, len(dic))
            items = dic.items()

            self.assertIn(("downloads", 300), items)
            self.assertIn(("cats", 5), items)

    def test_session_query_include_counter_and_document(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.store(Order(company="companies/2-A"), "orders/2-A")
            session.store(Order(company="companies/3-A"), "orders/3-A")

            session.store(Company(name="HR"), "companies/1-A")
            session.store(Company(name="HP"), "companies/2-A")
            session.store(Company(name="Google"), "companies/3-A")

            session.counters_for("orders/1-A").increment("downloads", 100)
            session.counters_for("orders/2-A").increment("downloads", 200)
            session.counters_for("orders/3-A").increment("downloads", 300)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(
                lambda i: i.include_counter("downloads").include_documents(path="company")
            )
            self.assertEqual("from 'Orders' include company,counters('downloads')", query._to_string())

            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests)

            session.load(["companies/1-A", "companies/2-A", "companies/3-A"], Company)

            order = query_result[0]
            self.assertEqual("orders/1-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(100, val)

            order = query_result[1]
            self.assertEqual("orders/2-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(200, val)

            order = query_result[2]
            self.assertEqual("orders/3-A", order.Id)
            val = session.counters_for(order).get("downloads")
            self.assertEqual(300, val)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_session_query_include_all_counters_of_document_and_of_related_document(self):
        with self.store.open_session() as session:
            session.store(Order(employee="employees/1-A"), "orders/1-A")
            session.store(Order(employee="employees/2-A"), "orders/2-A")
            session.store(Order(employee="employees/3-A"), "orders/3-A")
            session.store(Employee(first_name="Aviv"), "employees/1-A")
            session.store(Employee(first_name="Jerry"), "employees/2-A")
            session.store(Employee(first_name="Bob"), "employees/3-A")

            session.counters_for("orders/1-A").increment("likes", 100)
            session.counters_for("orders/2-A").increment("likes", 200)
            session.counters_for("orders/3-A").increment("likes", 300)
            session.counters_for("orders/1-A").increment("downloads", 1000)
            session.counters_for("orders/2-A").increment("downloads", 2000)

            session.counters_for("employees/1-A").increment("likes", 100)
            session.counters_for("employees/2-A").increment("likes", 200)
            session.counters_for("employees/3-A").increment("likes", 300)
            session.counters_for("employees/1-A").increment("downloads", 1000)
            session.counters_for("employees/2-A").increment("downloads", 2000)
            session.counters_for("employees/3-A").increment("cats", 5)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=Order).include(
                lambda i: i.include_all_counters().include_all_counters(path="employee")
            )
            self.assertEqual("from 'Orders' include counters(),counters(employee)", query._to_string())
            orders = list(query.order_by("employee"))
            self.assertEqual(1, session.advanced.number_of_requests)

            order = orders[0]
            self.assertEqual("orders/1-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(2, len(dic))
            self.assertSequenceContainsElements(dic.items(), ("likes", 100), ("downloads", 1000))

            order = orders[1]
            self.assertEqual("orders/2-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(2, len(dic))
            self.assertSequenceContainsElements(dic.items(), ("likes", 200), ("downloads", 2000))

            order = orders[2]
            self.assertEqual("orders/3-A", order.Id)
            dic = session.counters_for(order).get_all()
            self.assertEqual(1, len(dic))
            self.assertIn(("likes", 300), dic.items())

            dic = session.counters_for("employees/1-A").get_all()
            self.assertEqual(2, len(dic))
            self.assertSequenceContainsElements(dic.items(), ("likes", 100), ("downloads", 1000))

            dic = session.counters_for("employees/2-A").get_all()
            self.assertEqual(2, len(dic))
            self.assertSequenceContainsElements(dic.items(), ("likes", 200), ("downloads", 2000))

            dic = session.counters_for("employees/3-A").get_all()
            self.assertEqual(2, len(dic))
            self.assertSequenceContainsElements(dic.items(), ("likes", 300), ("cats", 5))

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_counters_should_be_cached_on_all_docs_collection(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"))
            session.counters_for("orders/1-A").increment("downloads", 100)
            session.save_changes()

        with self.store.open_session() as session:
            list(session.advanced.raw_query("from @all_docs include counters($p0)").add_parameter("p0", ["downloads"]))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(100, counter_value)
            session.counters_for("orders/1-A").increment("downloads", 200)
            session.save_changes()

            list(session.advanced.raw_query("from @all_docs include counters()"))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(300, counter_value)
            session.counters_for("orders/1-A").increment("downloads", 200)
            session.save_changes()

            list(session.advanced.raw_query("from @all_docs include counters($p0)").add_parameter("p0", ["downloads"]))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(500, counter_value)

    def test_counters_should_be_cached_on_collection(self):
        with self.store.open_session() as session:
            session.store(Order(company="companies/1-A"), "orders/1-A")
            session.counters_for("orders/1-A").increment("downloads", 100)
            session.save_changes()

        with self.store.open_session() as session:
            list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads")))
            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(100, counter_value)

            session.counters_for("orders/1-A").increment("downloads", 200)
            session.save_changes()

            list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads")))

            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(300, counter_value)

            session.counters_for("orders/1-A").increment("downloads", 200)
            session.save_changes()

            list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads")))

            counter_value = session.counters_for("orders/1-A").get("downloads")
            self.assertEqual(500, counter_value)

        with self.store.open_session() as session:
            session.load("orders/1-A", Order)

    def test_counters_caching_should_handle_deletion__include_counters(self):
        self._counters_caching_should_handle_deletion(
            lambda session: list(
                session.query(object_type=Order).include(
                    lambda i: i.include_counters("downloads", "uploads", "bugs")
                )
            ),
            None,
        )

    def test_counters_caching_should_handle_deletion__include_counter_upload(self):
        self._counters_caching_should_handle_deletion(
            lambda session: list(
                session.query(object_type=Order).include(lambda i: i.include_counters("uploads"))
            ),
            300,
        )

    def test_counters_caching_should_handle_deletion__include_counter_download(self):
        self._counters_caching_should_handle_deletion(
            lambda session: list(session.query(object_type=Order).include(lambda i: i.include_counters("downloads"))),
            None,
        )

    def test_counters_caching_should_handle_deletion__include_all_counters(self):
        self._counters_caching_should_handle_deletion(
            lambda session: list(session.query(object_type=Order).include(lambda i: i.include_all_counters())),
            None,
        )

    class __CounterResult:
        def __init__(self, downloads: int = None, likes: int = None, name: str = None):
            self.downloads = downloads
            self.likes = likes
            self.name = name

    class __CounterResult4:
        def __init__(self, downloads: int = None, name: str = None, likes: Dict[str, int] = None):
            self.downloads = downloads
            self.name = name
            self.likes = likes

    def test_raw_query_select_single_counter(self):
        with self.store.open_session() as session:
            session.store(User(name="Jerry"), "users/1-A")
            session.store(User(name="Bob"), "users/2-A")
            session.store(User(name="Pigpen"), "users/3-A")

            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/2-A").increment("downloads", 200)
            session.counters_for("users/3-A").increment("likes", 300)

            session.save_changes()

        with self.store.open_session() as session:
            query = list(
                session.advanced.raw_query(
                    'from users select counter("downloads") as downloads', object_type=self.__CounterResult
                )
            )

            self.assertEqual(3, len(query))
            # Results are coming up in random order
            downloads = list(map(lambda r: r.downloads, query))
            self.assertSequenceContainsElements(downloads, 100, 200, None)

    def test_raw_query_select_single_counter_with_doc_alias(self):
        with self.store.open_session() as session:
            session.store(User(name="Jerry"), "users/1-A")
            session.store(User(name="Bob"), "users/2-A")
            session.store(User(name="Pigpen"), "users/3-A")

            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/2-A").increment("downloads", 200)
            session.counters_for("users/3-A").increment("likes", 300)

            session.save_changes()

        with self.store.open_session() as session:
            query = list(
                session.advanced.raw_query(
                    'from users as u select counter(u, "downloads") as downloads', object_type=self.__CounterResult
                )
            )
            self.assertEqual(3, len(query))
            # Results are coming up in random order
            downloads = list(map(lambda r: r.downloads, query))
            self.assertSequenceContainsElements(downloads, 100, 200, None)

    def test_raw_query_select_multiple_counters(self):
        with self.store.open_session() as session:
            session.store(User(name="Jerry"), "users/1-A")
            session.store(User(name="Bob"), "users/2-A")
            session.store(User(name="Pigpen"), "users/3-A")

            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/1-A").increment("likes", 200)
            session.counters_for("users/2-A").increment("downloads", 400)
            session.counters_for("users/2-A").increment("likes", 800)
            session.counters_for("users/3-A").increment("likes", 1600)

            session.save_changes()

        with self.store.open_session() as session:
            query = list(
                session.advanced.raw_query(
                    'from users select counter("downloads"), counter("likes")', object_type=self.__CounterResult
                )
            )

            self.assertEqual(3, len(query))
            # Results are coming up in random order
            downloads = list(map(lambda r: r.downloads, query))
            likes = list(map(lambda r: r.likes, query))
            self.assertSequenceContainsElements(likes, 200, 800, 1600)
            self.assertSequenceContainsElements(downloads, 100, 400, None)

    def test_raw_query_simple_projection_with_counter(self):
        with self.store.open_session() as session:
            session.store(User(name="Jerry"), "users/1-A")
            session.store(User(name="Bob"), "users/2-A")
            session.store(User(name="Pigpen"), "users/3-A")

            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/2-A").increment("downloads", 200)
            session.counters_for("users/3-A").increment("likes", 400)

            session.save_changes()

        with self.store.open_session() as session:
            query = list(
                session.advanced.raw_query(
                    "from users select name, counter('downloads')", object_type=self.__CounterResult
                )
            )
            self.assertEqual(3, len(query))
            downloads = list(map(lambda r: r.downloads, query))
            names = list(map(lambda r: r.name, query))
            self.assertSequenceContainsElements(names, "Jerry", "Bob", "Pigpen")
            self.assertSequenceContainsElements(downloads, 100, 200, None)

    def test_raw_query_js_with_counter_raw_values(self):
        with self.store.open_session() as session:
            session.store(User(name="Jerry"), "users/1-A")
            session.store(User(name="Bob"), "users/2-A")
            session.store(User(name="Pigpen"), "users/3-A")

            session.counters_for("users/1-A").increment("downloads", 100)
            session.counters_for("users/1-A").increment("likes", 200)
            session.counters_for("users/2-A").increment("downloads", 300)
            session.counters_for("users/2-A").increment("likes", 400)
            session.counters_for("users/3-A").increment("likes", 500)

            session.save_changes()

        with self.store.open_session() as session:
            query = list(
                session.advanced.raw_query(
                    "from 'Users' as u "
                    "select { name: u.name, downloads: counter(u, 'downloads'), likes: counterRaw(u, 'likes') }",
                    object_type=self.__CounterResult4,
                )
            )

            self.assertEqual(3, len(query))
            downloads = list(map(lambda r: r.downloads, query))
            names = list(map(lambda r: r.name, query))
            likes = list(map(lambda r: r.likes.popitem()[1], query))
            self.assertSequenceContainsElements(names, "Jerry", "Bob", "Pigpen")
            self.assertSequenceContainsElements(downloads, 100, 300, None)
            self.assertSequenceContainsElements(likes, 200, 400, 500)
