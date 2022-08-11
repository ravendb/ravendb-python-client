from __future__ import annotations

from enum import Enum
from ravendb import AbstractIndexCreationTask
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
    def __init__(self, currency: Currency, product: str, total: float, region: int):
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
