from __future__ import annotations

import datetime
from enum import Enum
from typing import List

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


class ItemsOrder:
    def __init__(self, items: List[str], at: datetime.datetime):
        self.items = items
        self.at = at


class ItemsOrders_All(AbstractIndexCreationTask):
    def __init__(self):
        super(ItemsOrders_All, self).__init__()
        self.map = "docs.ItemsOrders.Select(order => new { order.at, \n" "                          order.items })"


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

    def test_can_correctly_aggregate_multiple_items(self):
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
            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda x: x.by_field("product").sum_on("total"))
                .and_aggregate_by(lambda x: x.by_field("currency").sum_on("total"))
                .execute()
            )

            facet_result = r.get("product")
            self.assertEqual(2, len(facet_result.values))

            self.assertEqual(12, list(filter(lambda x: x.range_ == "milk", facet_result.values))[0].sum_)
            self.assertEqual(3333, list(filter(lambda x: x.range_ == "iphone", facet_result.values))[0].sum_)

            facet_result = r.get("currency")
            self.assertEqual(2, len(facet_result.values))

            self.assertEqual(3336, list(filter(lambda x: x.range_ == "eur", facet_result.values))[0].sum_)
            self.assertEqual(9, list(filter(lambda x: x.range_ == "nis", facet_result.values))[0].sum_)

    def test_can_correctly_aggregate_multiple_aggregations(self):
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
            r = (
                session.query_index_type(Orders_All, Order)
                .aggregate_by(lambda f: f.by_field("product").max_on("total").min_on("total"))
                .execute()
            )

            facet_result = r.get("product")
            self.assertEqual(2, len(facet_result.values))

            self.assertEqual(9, list(filter(lambda x: x.range_ == "milk", facet_result.values))[0].max_)
            self.assertEqual(3, list(filter(lambda x: x.range_ == "milk", facet_result.values))[0].min_)

            self.assertEqual(3333, list(filter(lambda x: x.range_ == "iphone", facet_result.values))[0].max_)
            self.assertEqual(3333, list(filter(lambda x: x.range_ == "iphone", facet_result.values))[0].min_)

    def test_can_correctly_aggregate_date_time_data_type_with_range_counts(self):
        ItemsOrders_All().execute(self.store)

        with self.store.open_session() as session:
            item1 = ItemsOrder(["first", "second"], datetime.datetime(3333, 3, 3, 3, 3, 3, 3))
            item2 = ItemsOrder(["first", "second"], datetime.datetime(3333, 3, 1, 3, 3, 3, 3))
            item3 = ItemsOrder(["first", "second"], datetime.datetime(3333, 3, 3, 3, 3, 3, 3))
            item4 = ItemsOrder(["first"], datetime.datetime(3333, 3, 3, 3, 3, 3, 3))

            session.store(item1)
            session.store(item2)
            session.store(item3)
            session.store(item4)
            session.save_changes()

        items = ["second"]
        min_value = datetime.datetime(1980, 3, 3, 3, 3, 3, 3)
        end0 = datetime.datetime(3333, 3, 1, 3, 3, 3, 3)
        end1 = datetime.datetime(3333, 3, 2, 3, 3, 3, 3)
        end2 = datetime.datetime(3333, 3, 3, 3, 3, 3, 4)

        self.wait_for_indexing(self.store)

        builder = RangeBuilder.for_path("at")
        with self.store.open_session() as session:
            r = (
                session.query_index_type(ItemsOrders_All, ItemsOrder)
                .where_greater_than_or_equal("at", end0)
                .aggregate_by(
                    lambda f: f.by_ranges(
                        builder.is_greater_than_or_equal_to(min_value),  # all - 4
                        builder.is_greater_than_or_equal_to(end0).is_less_than(end1),  # 0
                        builder.is_greater_than_or_equal_to(end1).is_less_than(end2),  # 1
                    )
                )
                .execute()
            )

        facet_results = r.get("at").values

        self.assertEqual(4, facet_results[0].count_)
        self.assertEqual(1, facet_results[1].count_)
        self.assertEqual(3, facet_results[2].count_)
