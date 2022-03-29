from typing import Dict, List

from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase, User


class TestLazy(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_lazily_load_entity(self):
        with self.store.open_session() as session:
            for i in range(1, 7):
                company = Company(f"companies/{i}")
                session.store(company, f"companies/{i}")

            session.save_changes()

        with self.store.open_session() as session:
            lazy_order = session.advanced.lazily.load(Company, "companies/1")
            self.assertFalse(lazy_order.is_value_created)

            order: Company = lazy_order.value
            self.assertEqual("companies/1", order.Id)

            lazy_orders = session.advanced.lazily.load(Company, ["companies/1", "companies/2"])
            self.assertFalse(lazy_orders.is_value_created)

            orders: Dict[str, Company] = lazy_orders.value
            self.assertEqual(2, len(orders))

            company1 = orders.get("companies/1")
            company2 = orders.get("companies/2")

            self.assertIsNotNone(company1)
            self.assertIsNotNone(company2)

            self.assertEqual("companies/1", company1.Id)
            self.assertEqual("companies/2", company2.Id)

            lazy_order = session.advanced.lazily.load(Company, "companies/3")

            self.assertFalse(lazy_order.is_value_created)

            order = lazy_order.value

            self.assertEqual("companies/3", order.Id)

            load = session.advanced.lazily.load(Company, ["no_such_1", "no_such_2"])
            missing_itmes = load.value

            self.assertIsNone(missing_itmes.get("no_such_1"))
            self.assertIsNone(missing_itmes.get("no_such_2"))

    def test_can_use_cache_when_lazy_loading(self):
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.lazily.load(User, "users/1").value
            old_request_count = session.number_of_requests

            user: User = session.advanced.lazily.load(User, "users/1").value
            self.assertEqual("Oren", user.name)

            self.assertEqual(old_request_count, session.number_of_requests)

    def test_can_execute_all_pending_lazy_operations(self):
        with self.store.open_session() as session:
            company1 = Company("companies/1")
            session.store(company1, "companies/1")

            company2 = Company("companies/2")
            session.store(company2, "companies/2")

            session.save_changes()

        with self.store.open_session() as session:
            company1ref = []
            company2ref = []

            def __fun(company_list: List):
                def __inner(x):
                    company_list.append(x)

                return __inner

            session.advanced.lazily.load(Company, "companies/1", lambda x: __fun(company1ref)(x))
            session.advanced.lazily.load(Company, "companies/2", lambda x: __fun(company2ref)(x))

            self.assertEqual(0, len(company1ref))
            self.assertEqual(0, len(company2ref))

            session.advanced.eagerly.execute_all_pending_lazy_operations()

            self.assertEqual(1, len(company1ref))
            self.assertEqual(1, len(company2ref))

            self.assertEqual("companies/1", company1ref[0].Id)
            self.assertEqual("companies/2", company2ref[0].Id)

    def test_with_queued_actions_load(self):
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user_ref = []

            def __fun(x):
                user_ref.append(x)

            session.advanced.lazily.load(User, "users/1", lambda x: __fun(x))

            session.advanced.eagerly.execute_all_pending_lazy_operations()

            self.assertEqual(1, len(user_ref))

    def test_dont_lazy_load_already_loaded_values(self):
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/1")

            user2 = User("Marcin")
            session.store(user2, "users/2")

            user3 = User("John")
            session.store(user3, "users/3")

            session.save_changes()

        with self.store.open_session() as session:
            lazy_load = session.advanced.lazily.load(User, ["users/2", "users/3"])
            session.advanced.lazily.load(User, ["users/1", "users/3"])

            session.load("users/2", User)
            session.load("users/3", User)

            session.advanced.eagerly.execute_all_pending_lazy_operations()

            self.assertTrue(session.is_loaded("users/1"))

            users = lazy_load.value
            self.assertEqual(2, len(users))

            old_request_count = session.number_of_requests
            lazy_load = session.advanced.lazily.load(User, ["users/3"])
            session.advanced.eagerly.execute_all_pending_lazy_operations()

            self.assertEqual(old_request_count, session.number_of_requests)
