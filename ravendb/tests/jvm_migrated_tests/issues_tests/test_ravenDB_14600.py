from ravendb import DocumentStore, AbstractIndexCreationTask
from ravendb.infrastructure.orders import Employee, Order
from ravendb.tests.test_base import TestBase


class MyIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(MyIndex, self).__init__()
        self.map = "from o in docs.Orders select new { o.employee, o.company }"


class TestRavenDB14600(TestBase):
    def setUp(self):
        super(TestRavenDB14600, self).setUp()

    def _customize_store(self, store: DocumentStore) -> None:
        store.conventions.disable_topology_updates = True

    def test_can_include_facet_result(self):
        with self.store.open_session() as session:
            employee = Employee(first_name="Test")
            session.store(employee, "employees/1")

            order = Order(employee="employees/1", company="companies/1-A")
            session.store(order, "orders/1")
            session.save_changes()

        self.store.execute_index(MyIndex())
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            facets = (
                session.query_index_type(MyIndex, Order)
                .include("employee")
                .where_equals("company", "companies/1-A")
                .aggregate_by(lambda x: x.by_field("employee"))
                .execute()
            )

            self.assertIsNotNone(facets.get("employee").values)

            for f in facets.get("employee").values:
                session.load(f.range_, object)

            self.assertEqual(1, session.advanced.number_of_requests)
