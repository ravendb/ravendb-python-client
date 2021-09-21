from typing import Callable

from pyravendb.store.document_session import DocumentSession
from pyravendb.tests.test_base import TestBase, Order, Employee, Company, User


class TestQueryOnCounters(TestBase):
    def setUp(self):
        super(TestQueryOnCounters, self).setUp()

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
            query = session.query(object_type=Order).include(builder=lambda x: x.include_counters("downloads"))
            self.assertEqual("FROM Orders include counters('downloads')", str(query))
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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

            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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
            query = session.query(object_type=Order).include(builder=lambda x: x.include_counters("downloads", "likes"))
            self.assertIn(
                str(query),
                [
                    "FROM Orders include counters('downloads'),counters('likes')",
                    "FROM Orders include counters('likes'),counters('downloads')",
                ],
            )
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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

            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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
            query = session.query(object_type=Order).include(
                builder=lambda i: i.include_counters("downloads", path="employee")
            )
            self.assertEqual("FROM Orders include counters(employee, 'downloads')", str(query))

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
                builder=lambda i: i.include_counters("downloads", "likes", path="employee")
            )

            self.assertIn(
                str(query),
                [
                    "FROM Orders include counters(employee, 'downloads'),counters(employee, 'likes')",
                    "FROM Orders include counters(employee, 'likes'),counters(employee, 'downloads')",
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
                builder=lambda i: i.include_counters("likes").include_counters("downloads", path="employee")
            )
            self.assertEqual("FROM Orders include counters('likes'),counters(employee, 'downloads')", str(query))

            orders = list(query.order_by("employee"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())
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
            query = session.query(Order).include(builder=lambda i: i.include_all_counters())
            self.assertEqual("FROM Orders include counters()", str(query))
            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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
            query = session.query(Order).include(
                builder=lambda i: i.include_counters("downloads").include_documents(path="company")
            )
            self.assertEqual("FROM Orders include company,counters('downloads')", str(query))

            query_result = list(query.order_by("company"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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

            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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
                builder=lambda i: i.include_all_counters().include_all_counters(path="employee")
            )
            self.assertEqual("FROM Orders include counters(),counters(employee)", str(query))
            orders = list(query.order_by("employee"))
            self.assertEqual(1, session.advanced.number_of_requests_in_session())

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

            self.assertEqual(1, session.advanced.number_of_requests_in_session())
