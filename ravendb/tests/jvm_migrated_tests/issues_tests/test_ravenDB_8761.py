from typing import List

from ravendb.documents.queries.group_by import GroupBy
from ravendb.infrastructure.orders import Order, OrderLine, Address
from ravendb.tests.test_base import TestBase


class ProductCount:
    def __init__(
        self,
        product_name: str = None,
        count: int = None,
        country: str = None,
        quantity: int = None,
        products: List[str] = None,
        quantities: List[int] = None,
    ):
        self.product_name = product_name
        self.count = count
        self.country = country
        self.quantity = quantity
        self.products = products
        self.quantities = quantities


class TestRavenDB8761(TestBase):
    def setUp(self):
        super(TestRavenDB8761, self).setUp()

    def put_docs(self):
        with self.store.open_session() as session:
            order1 = Order()

            order_line_1 = OrderLine("products/1", quantity=1)
            order_line_2 = OrderLine("products/2", quantity=2)
            order1.lines = [order_line_1, order_line_2]

            addres1 = Address(country="USA")

            order1.ship_to = addres1
            session.store(order1)

            order_line_21 = OrderLine("products/2", quantity=3)

            address2 = Address(country="USA")
            order2 = Order(lines=[order_line_21], ship_to=address2)
            session.store(order2)

            session.save_changes()

    def test_can_group_by_array_values(self):
        self.put_docs()

        with self.store.open_session() as session:
            product_counts_1 = list(
                session.advanced.raw_query(
                    "from Orders group by lines[].product\n"
                    "  order by count()\n"
                    "  select key() as product_name, count() as count",
                    ProductCount,
                ).wait_for_non_stale_results()
            )

            product_counts_2 = list(
                session.advanced.document_query(object_type=Order)
                .group_by("lines[].product")
                .select_key(None, "product_name")
                .select_count()
                .of_type(ProductCount)
            )

            for products in [product_counts_1, product_counts_2]:
                self.assertEqual(2, len(products))

                self.assertEqual("products/1", products[0].product_name)
                self.assertEqual(1, products[0].count)

                self.assertEqual("products/2", products[1].product_name)
                self.assertEqual(2, products[1].count)

        with self.store.open_session() as session:
            product_counts_1 = list(
                (
                    session.advanced.raw_query(
                        "from Orders\n"
                        " group by lines[].product, ship_to.country\n"
                        " order by count() \n"
                        " select lines[].product as product_name, ship_to.country as country, count() as count",
                        ProductCount,
                    )
                )
            )

            product_counts_2 = list(
                (
                    session.advanced.document_query(object_type=Order)
                    .group_by("lines[].product", "ship_to.country")
                    .select_key("lines[].product", "product_name")
                    .select_key("ship_to.country", "country")
                    .select_count()
                    .of_type(ProductCount)
                )
            )

            for products in [product_counts_1, product_counts_2]:
                self.assertEqual(2, len(products))

                self.assertEqual("products/1", products[0].product_name)
                self.assertEqual(1, products[0].count)
                self.assertEqual("USA", products[0].country)

                self.assertEqual("products/2", products[1].product_name)
                self.assertEqual(2, products[1].count)
                self.assertEqual("USA", products[1].country)

        with self.store.open_session() as session:
            product_counts_1 = list(
                session.advanced.raw_query(
                    "from Orders\n"
                    " group by lines[].product, lines[].quantity\n"
                    " order by lines[].quantity\n"
                    " select lines[].product as product_name, lines[].quantity as quantity, count() as count",
                    ProductCount,
                )
            )

            product_counts_2 = list(
                session.advanced.document_query(object_type=Order)
                .group_by("lines[].product", "lines[].quantity")
                .select_key("lines[].product", "product_name")
                .select_key("lines[].quantity", "quantity")
                .select_count()
                .of_type(ProductCount)
            )

        for products in [product_counts_1, product_counts_2]:
            self.assertEqual(3, len(products))
            self.assertEqual(1, products[0].count)
            self.assertEqual(1, products[0].quantity)

            self.assertEqual("products/2", products[1].product_name)
            self.assertEqual(1, products[1].count)
            self.assertEqual(2, products[1].quantity)

            self.assertEqual("products/2", products[2].product_name)
            self.assertEqual(1, products[2].count)
            self.assertEqual(3, products[2].quantity)

    def test_can_group_by_array_content(self):
        self.put_docs()

        with self.store.open_session() as session:
            order_line1 = OrderLine("products/1", quantity=1)
            order_line2 = OrderLine("products/2", quantity=2)
            address = Address(country="USA")

            order = Order(ship_to=address, lines=[order_line1, order_line2])

            session.store(order)
            session.save_changes()

        with self.store.open_session() as session:
            product_counts1 = list(
                session.advanced.raw_query(
                    "from Orders group by array(lines[].product)\n"
                    " order by count()\n"
                    " select key() as products, count() as count",
                    ProductCount,
                ).wait_for_non_stale_results()
            )

            product_counts2 = list(
                session.advanced.document_query(object_type=Order)
                .group_by(GroupBy.array("lines[].product"))
                .select_key(None, "products")
                .select_count()
                .order_by("count")
                .of_type(ProductCount)
            )

            for products in [product_counts1, product_counts2]:
                self.assertEqual(2, len(products))

                self.assertEqual(1, len(products[0].products))
                self.assertIn("products/2", products[0].products)
                self.assertEqual(1, products[0].count)

                self.assertEqual(2, len(products[1].products))
                self.assertIn("products/1", products[1].products)
                self.assertIn("products/2", products[1].products)
                self.assertEqual(2, products[1].count)

        with self.store.open_session() as session:
            product_counts1 = list(
                session.advanced.raw_query(
                    "from Orders\n"
                    " group by array(lines[].product), ship_to.country\n"
                    " order by count() \n"
                    " select lines[].product as products, ship_to.country as country, count() as count",
                    ProductCount,
                ).wait_for_non_stale_results()
            )

            product_counts2 = list(
                session.advanced.document_query(object_type=Order)
                .group_by(GroupBy.array("lines[].product"), GroupBy.field("ship_to.country"))
                .select_key("lines[].product", "products")
                .select_count()
                .order_by("count")
                .of_type(ProductCount)
            )

            for products in [product_counts1, product_counts2]:
                self.assertEqual(2, len(products))

                self.assertEqual(1, len(products[0].products))
                self.assertIn("products/2", products[0].products)
                self.assertEqual(1, products[0].count)

                self.assertEqual(2, len(products[1].products))
                self.assertIn("products/1", products[1].products)
                self.assertIn("products/2", products[1].products)
                self.assertEqual(2, products[1].count)

        with self.store.open_session() as session:
            product_counts1 = list(
                session.advanced.raw_query(
                    "from Orders\n"
                    " group by array(lines[].product), array(lines[].quantity)\n"
                    " order by count()\n"
                    " select lines[].product as products, lines[].quantity as quantities, count() as count",
                    ProductCount,
                ).wait_for_non_stale_results()
            )

            product_counts2 = list(
                session.advanced.document_query(object_type=Order)
                .group_by(GroupBy.array("lines[].product"), GroupBy.array("lines[].quantity"))
                .select_key("lines[].product", "products")
                .select_key("lines[].quantity", "quantities")
                .select_count()
                .order_by("count")
                .of_type(ProductCount)
            )

            for products in [product_counts1, product_counts2]:
                self.assertEqual(2, len(products))

                self.assertEqual(1, len(products[0].products))
                self.assertIn("products/2", products[0].products)
                self.assertEqual(1, products[0].count)
                self.assertEqual(1, len(products[0].quantities))
                self.assertIn(3, products[0].quantities)

                self.assertEqual(2, len(products[1].products))
                self.assertIn("products/1", products[1].products)
                self.assertIn("products/2", products[1].products)
                self.assertEqual(2, products[1].count)
                self.assertEqual(2, len(products[1].quantities))
                self.assertIn(1, products[1].quantities)
                self.assertIn(2, products[1].quantities)
