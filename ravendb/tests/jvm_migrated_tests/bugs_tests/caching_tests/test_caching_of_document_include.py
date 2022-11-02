from typing import List

from ravendb.infrastructure.orders import Order, OrderLine, Product
from ravendb.tests.test_base import TestBase


class User:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        partner_id: str = None,
        email: str = None,
        tags: List[str] = None,
        age: int = None,
        active: bool = None,
    ):
        self.Id = Id
        self.name = name
        self.partner_id = partner_id
        self.email = email
        self.tags = tags
        self.age = age
        self.active = active


class CachingOfDocumentsIncludeTest(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheLazy(self):
        with self.store.open_session() as session:
            user = User(name="Ayende")
            session.store(user)

            partner = User(partner_id="users/1-A")
            session.store(partner)

            session.save_changes()

        with self.store.open_session() as session:
            user: User = session.load("users/2-A", User)
            session.load(user.partner_id)

            old = session.number_of_requests
            new_user = session.include("partner_id").load(User, "users/2-A")
            self.assertEqual(old, session.number_of_requests)

    def test_can_include_nested_paths(self):
        order = Order()

        order_line1 = OrderLine(product="products/1-A", product_name="phone")
        order_line2 = OrderLine(product="products/2-A", product_name="mouse")

        order.lines = [order_line1, order_line2]

        product1 = Product("products/1-A", "phone")
        product2 = Product("products/2-A", "mouse")

        with self.store.open_session() as session:
            session.store(order, "orders/1-A")
            session.store(product1)
            session.store(product2)
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.number_of_requests)
            orders = list(session.query(object_type=Order).include(lambda x: x.include_documents("lines[].product")))

            self.assertEqual(1, session.advanced.number_of_requests)

            product = session.load(orders[0].lines[0]["product"], Product)
            self.assertEqual(1, session.advanced.number_of_requests)

        with self.store.open_session() as session:
            self.assertEqual(0, session.advanced.number_of_requests)
            orders = list(session.query(object_type=Order).include(lambda x: x.include_documents("lines.product")))
            self.assertEqual(1, session.advanced.number_of_requests)

            product = session.load(orders[0].lines[0]["product"], Product)
            self.assertEqual(1, session.advanced.number_of_requests)

            product = session.load(orders[0].lines[0]["product"])
            self.assertEqual(1, session.advanced.number_of_requests)
