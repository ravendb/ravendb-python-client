from __future__ import annotations

from enum import Enum
from ravendb import AbstractIndexCreationTask, RangeBuilder
from ravendb.tests.test_base import TestBase


class Orders_All(AbstractIndexCreationTask):
    def __init__(self):
        super(Orders_All, self).__init__()
        self.map = (
            "docs.Orders.Select(order => new { order.currency, \n"
            "                          order.product,\n"
            "                          order.total,\n"
            "                          order.quantity,\n"
            "                          order.region,\n"
            "                          order.at,\n"
            "                          order.tax })"
        )


class Order:
    def __init__(self, currency: Currency = None, product: str = None, total: float = None, region: int = None):
        self.currency = currency
        self.product = product
        self.total = total
        self.region = region


class Currency(Enum):
    EUR = "EUR"
    PLN = "PLN"
    NIS = "NIS"


class TestAggregation(TestBase):
    def setUp(self):
        super(TestAggregation, self).setUp()

    def test_can_correctly_aggregate_double(self):
        Orders_All().execute(self.store)

        with self.store.open_session() as session:
            obj = Order(Currency.EUR, "Milk", 1.1, 1)
            obj2 = Order(Currency.EUR, "Milk", 1, 1)

            session.store(obj)
            session.store(obj2)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            result = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("region").max_on("total").min_on("total"))
                .execute()
            )
            facet_result = result.get("region")
            self.assertEqual(2, facet_result.values[0].count_)

            self.assertEqual(1, facet_result.values[0].min_)
            self.assertEqual(1.1, facet_result.values[0].max_)
            self.assertEqual(1, len(list(filter(lambda x: "1" == x.range_, facet_result.values))))

    def test_can_correctly_aggregate_ranges(self):
        Orders_All().execute(self.store)

        with self.store.open_session() as session:
            obj = Order(Currency.EUR, "Milk", 3)
            obj2 = Order(Currency.NIS, "Milk", 9)
            obj3 = Order(Currency.EUR, "iPhone", 3333)

            session.store(obj)
            session.store(obj2)
            session.store(obj3)

            session.save_changes()

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            range = RangeBuilder.for_path("total")
            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("product").sum_on("total"))
                .and_aggregate_by(
                    lambda f: f.by_ranges(
                        range.is_less_than(100),
                        range.is_greater_than_or_equal_to(100).is_less_than(500),
                        range.is_greater_than_or_equal_to(500).is_less_than(500),
                        range.is_greater_than_or_equal_to(1500),
                    ).sum_on("total")
                )
                .execute()
            )

            facet_result = r.get("product")
            self.assertEqual(2, len(facet_result.values))

            self.assertEqual(12, list(filter(lambda x: x.range_ == "milk", facet_result.values))[0].sum_)
            self.assertEqual(3333, list(filter(lambda x: x.range_ == "iphone", facet_result.values))[0].sum_)

            facet_result = r.get("total")
            self.assertEqual(4, len(facet_result.values))

            self.assertEqual(12, list(filter(lambda x: x.range_ == "total < 100", facet_result.values))[0].sum_)
            self.assertEqual(3333, list(filter(lambda x: x.range_ == "total >= 1500", facet_result.values))[0].sum_)
