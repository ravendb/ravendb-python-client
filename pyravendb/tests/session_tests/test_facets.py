from pyravendb.tests.test_base import TestBase
from pyravendb.data.query import Facet, FacetMode
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation
from pyravendb.data.indexes import IndexDefinition
import unittest


class Product(object):
    def __init__(self, name, price_per_unit):
        self.name = name
        self.price_per_unit = price_per_unit


class ProductsAndPricePerUnit:
    def __init__(self):
        self.maps = ("from product in docs.Products "
                     "select new {"
                     "price_per_unit = product.price_per_unit}")
        self.index_definition = IndexDefinition(name=ProductsAndPricePerUnit.__name__, maps=self.maps)

    def execute(self, store):
        store.maintenance.send(PutIndexesOperation(self.index_definition))


class TestFacets(TestBase):
    def setUp(self):
        super(TestFacets, self).setUp()
        with self.store.open_session() as session:
            session.store(Product("TV", 1022))
            session.store(Product("jacket", 100))
            session.save_changes()

        ProductsAndPricePerUnit().execute(self.store)
        self.facets = [Facet("Products", mode=FacetMode.ranges),
                       Facet("price_per_unit_L_Range", ranges=["{100 TO 3000]", "[402 TO 2000]"], mode=FacetMode.ranges)]

    def tearDown(self):
        super(TestFacets, self).tearDown()
        self.delete_all_topology_files()

    def test_facets_with_documents(self):
        with self.store.open_session() as session:
            query_results = session.query(object_type=Product,
                                          index_name=ProductsAndPricePerUnit.__name__).where_greater_than(
                "price_per_unit", 99).to_facets(self.facets)
            assert len(query_results['Results']) > 0


if __name__ == "__main__":
    unittest.main()
