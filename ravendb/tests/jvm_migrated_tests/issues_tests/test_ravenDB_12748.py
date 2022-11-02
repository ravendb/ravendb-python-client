from ravendb import AbstractIndexCreationTask, RangeBuilder
from ravendb.infrastructure.faceted import Order, Currency
from ravendb.tests.test_base import TestBase


class Orders_All(AbstractIndexCreationTask):
    def __init__(self):
        super(Orders_All, self).__init__()
        self.map = (
            "docs.Orders.Select(order => new { order.currency,\n"
            "                          order.product,\n"
            "                          order.total,\n"
            "                          order.quantity,\n"
            "                          order.region,\n"
            "                          order.at,\n"
            "                          order.tax })"
        )


class TestRavenDB12748(TestBase):
    def setUp(self):
        super(TestRavenDB12748, self).setUp()

    def test_can_correctly_aggregate(self):
        Orders_All().execute(self.store)
        with self.store.open_session() as session:
            order1 = Order()
            order1.currency = Currency.EUR
            order1.product = "Milk"
            order1.quantity = 3
            order1.total = 3

            session.store(order1)

            order2 = Order(currency=Currency.NIS, product="Milk", quantity=5, total=9)
            session.store(order2)

            order3 = Order(currency=Currency.EUR, product="iPhone", quantity=7777, total=3333)
            session.store(order3)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("region"))
                .and_aggregate_by(
                    lambda f: f.by_field("product").sum_on("total").average_on("total").sum_on("quantity")
                )
                .execute()
            )

            facet_result = r.get("region")
            self.assertEqual(1, len(facet_result.values))
            self.assertIsNone(facet_result.values[0].name)
            self.assertEqual(3, facet_result.values[0].count_)

            facet_result = r.get("product")
            total_values = list(filter(lambda x: x.name == "total", facet_result.values))
            self.assertEqual(2, len(total_values))

            milk_value = list(filter(lambda x: x.range_ == "milk", total_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", total_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(12, milk_value.sum_)
            self.assertEqual(3333, iphone_value.sum_)
            self.assertEqual(6, milk_value.average_)
            self.assertEqual(3333, iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

            quantity_values = list(filter(lambda x: x.name == "quantity", facet_result.values))

            milk_value = list(filter(lambda x: x.range_ == "milk", quantity_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", quantity_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(8, milk_value.sum_)
            self.assertEqual(7777, iphone_value.sum_)
            self.assertIsNone(milk_value.average_)
            self.assertIsNone(iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

        with self.store.open_session() as session:
            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("region"))
                .and_aggregate_by(
                    lambda f: f.by_field("product")
                    .sum_on("total", "T1")
                    .average_on("total", "T1")
                    .sum_on("quantity", "Q1")
                )
                .execute()
            )

            facet_result = r.get("region")
            self.assertEqual(1, len(facet_result.values))
            self.assertIsNone(facet_result.values[0].name)
            self.assertEqual(3, facet_result.values[0].count_)

            facet_result = r.get("product")
            total_values = list(filter(lambda x: x.name == "T1", facet_result.values))

            self.assertEqual(2, len(total_values))

            milk_value = list(filter(lambda x: x.range_ == "milk", total_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", total_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(12, milk_value.sum_)
            self.assertEqual(3333, iphone_value.sum_)
            self.assertEqual(6, milk_value.average_)
            self.assertEqual(3333, iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

            quantity_values = list(filter(lambda x: x.name == "Q1", facet_result.values))

            milk_value = list(filter(lambda x: x.range_ == "milk", quantity_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", quantity_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(8, milk_value.sum_)
            self.assertEqual(7777, iphone_value.sum_)
            self.assertIsNone(milk_value.average_)
            self.assertIsNone(iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("region"))
                .and_aggregate_by(
                    lambda f: f.by_field("product")
                    .sum_on("total", "T1")
                    .sum_on("total", "T2")
                    .average_on("total", "T2")
                    .sum_on("quantity", "Q1")
                )
                .execute()
            )

            facet_result = r.get("region")
            self.assertEqual(1, len(facet_result.values))
            self.assertIsNone(facet_result.values[0].name)
            self.assertEqual(3, facet_result.values[0].count_)

            facet_result = r.get("product")

            total_values = list(filter(lambda x: x.name == "T1", facet_result.values))
            self.assertEqual(2, len(total_values))

            milk_value = list(filter(lambda x: x.range_ == "milk", total_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", total_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(12, milk_value.sum_)
            self.assertEqual(3333, iphone_value.sum_)
            self.assertIsNone(milk_value.average_)
            self.assertIsNone(iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

            total_values = list(filter(lambda x: x.name == "T2", facet_result.values))

            self.assertEqual(2, len(total_values))

            milk_value = list(filter(lambda x: x.range_ == "milk", total_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", total_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(12, milk_value.sum_)
            self.assertEqual(3333, iphone_value.sum_)
            self.assertEqual(6, milk_value.average_)
            self.assertEqual(3333, iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

            quantity_values = list(filter(lambda x: x.name == "Q1", facet_result.values))

            milk_value = list(filter(lambda x: x.range_ == "milk", quantity_values))[0]
            iphone_value = list(filter(lambda x: x.range_ == "iphone", quantity_values))[0]

            self.assertEqual(2, milk_value.count_)
            self.assertEqual(1, iphone_value.count_)
            self.assertEqual(8, milk_value.sum_)
            self.assertEqual(7777, iphone_value.sum_)
            self.assertIsNone(milk_value.average_)
            self.assertIsNone(iphone_value.average_)
            self.assertIsNone(milk_value.max_)
            self.assertIsNone(iphone_value.max_)
            self.assertIsNone(milk_value.min_)
            self.assertIsNone(iphone_value.min_)

    def test_can_correctly_aggregate_ranges(self):
        Orders_All().execute(self.store)
        with self.store.open_session() as session:
            order1 = Order()
            order1.currency = Currency.EUR
            order1.product = "Milk"
            order1.quantity = 3
            order1.total = 3

            session.store(order1)

            order2 = Order(currency=Currency.NIS, product="Milk", quantity=5, total=9)
            session.store(order2)

            order3 = Order(currency=Currency.EUR, product="iPhone", quantity=7777, total=3333)
            session.store(order3)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            range = RangeBuilder.for_path("total")
            r = (
                session.query_index("Orders/All", Order)
                .aggregate_by(
                    lambda f: f.by_ranges(
                        range.is_less_than(100),
                        range.is_greater_than_or_equal_to(100).is_less_than(500),
                        range.is_greater_than_or_equal_to(500).is_less_than(1500),
                        range.is_greater_than_or_equal_to(1500),
                    )
                    .sum_on("total")
                    .average_on("total")
                    .sum_on("quantity")
                )
                .execute()
            )

            facet_result = r.get("total")

            self.assertEqual(8, len(facet_result.values))

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "total", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "total", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(12, range1.sum_)
            self.assertEqual(3333, range2.sum_)
            self.assertEqual(6, range1.average_)
            self.assertEqual(3333, range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "quantity", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "quantity", facet_result.values))[
                0
            ]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(8, range1.sum_)
            self.assertEqual(7777, range2.sum_)
            self.assertIsNone(range1.average_)
            self.assertIsNone(range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

        with self.store.open_session() as session:
            range = RangeBuilder.for_path("total")
            r = (
                session.query_index("Orders/All", Order)
                .aggregate_by(
                    lambda f: f.by_ranges(
                        range.is_less_than(100),
                        range.is_greater_than_or_equal_to(100).is_less_than(500),
                        range.is_greater_than_or_equal_to(500).is_less_than(1500),
                        range.is_greater_than_or_equal_to(1500),
                    )
                    .sum_on("total", "T1")
                    .average_on("total", "T1")
                    .sum_on("quantity", "Q1")
                )
                .execute()
            )

            facet_result = r.get("total")
            self.assertEqual(8, len(facet_result.values))

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "T1", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "T1", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(12, range1.sum_)
            self.assertEqual(3333, range2.sum_)
            self.assertEqual(6, range1.average_)
            self.assertEqual(3333, range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "Q1", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "Q1", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(8, range1.sum_)
            self.assertEqual(7777, range2.sum_)
            self.assertIsNone(range1.average_)
            self.assertIsNone(range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

        with self.store.open_session() as session:
            range = RangeBuilder.for_path("total")
            r = (
                session.query_index("Orders/All", Order)
                .aggregate_by(
                    lambda f: f.by_ranges(
                        range.is_less_than(100),
                        range.is_greater_than_or_equal_to(100).is_less_than(500),
                        range.is_greater_than_or_equal_to(500).is_less_than(1500),
                        range.is_greater_than_or_equal_to(1500),
                    )
                    .sum_on("total", "T1")
                    .sum_on("total", "T2")
                    .average_on("total", "T2")
                    .sum_on("quantity", "Q1")
                )
                .execute()
            )

            facet_result = r.get("total")
            self.assertEqual(12, len(facet_result.values))
            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "T1", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "T1", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(12, range1.sum_)
            self.assertEqual(3333, range2.sum_)
            self.assertIsNone(range1.average_)
            self.assertIsNone(range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "T2", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "T2", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(12, range1.sum_)
            self.assertEqual(3333, range2.sum_)
            self.assertEqual(6, range1.average_)
            self.assertEqual(3333, range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)

            range1 = list(filter(lambda x: x.range_ == "total < 100" and x.name == "Q1", facet_result.values))[0]
            range2 = list(filter(lambda x: x.range_ == "total >= 1500" and x.name == "Q1", facet_result.values))[0]

            self.assertEqual(2, range1.count_)
            self.assertEqual(1, range2.count_)
            self.assertEqual(8, range1.sum_)
            self.assertEqual(7777, range2.sum_)
            self.assertIsNone(range1.average_)
            self.assertIsNone(range2.average_)
            self.assertIsNone(range1.max_)
            self.assertIsNone(range2.max_)
            self.assertIsNone(range1.min_)
            self.assertIsNone(range2.min_)
