from ravendb.documents.operations.indexes import GetIndexOperation

from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask

from ravendb.tests.test_base import TestBase


class Orders_ProfitByProductAndOrderedAt(AbstractIndexCreationTask):
    def __init__(self, references_collection_name: str):
        super().__init__()
        self.map = """docs.Orders.SelectMany(order => order.Lines, (order, line) => new {
                        Product = line.Product,
                        OrderedAt = order.OrderedAt,
                        Profit = (((decimal) line.Quantity) * line.PricePerUnit) * (1M - line.Discount)
                    })"""

        self.reduce = """results.GroupBy(r => new {
                        OrderedAt = r.OrderedAt,
                        Product = r.Product
                    }).Select(g => new {
                        Product = g.Key.Product,
                        OrderedAt = g.Key.OrderedAt,
                        Profit = Enumerable.Sum(g, r => ((decimal) r.Profit))
                    })"""

        self._output_reduce_to_collection = "Profits"
        self._pattern_for_output_reduce_to_collection_references = "reports/daily/{OrderedAt:yyyy-MM-dd}"
        self._pattern_references_collection_name = references_collection_name


class TestRavenDB12932(TestBase):
    def setUp(self):
        super(TestRavenDB12932, self).setUp()

    def test_can_persist_pattern_for_output_reduce_to_collection_references(self):
        index_to_create = Orders_ProfitByProductAndOrderedAt("CustomCollection")
        index_to_create.execute(self.store)

        index_definition = self.store.maintenance.send(GetIndexOperation("Orders/ProfitByProductAndOrderedAt"))

        self.assertEqual(index_definition.output_reduce_to_collection, "Profits")
        self.assertEqual(index_definition.pattern_for_output_reduce_to_collection_references, "reports/daily/{OrderedAt:yyyy-MM-dd}")
        self.assertEqual(index_definition.pattern_references_collection_name, "CustomCollection")