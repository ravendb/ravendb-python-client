from threading import Semaphore

from ravendb.documents.subscriptions.options import SubscriptionCreationOptions, SubscriptionWorkerOptions
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.infrastructure.orders import Product, Category, Supplier
from ravendb.tests.test_base import TestBase

_reasonable_amount_of_time = 15


class TestRavenDB12257(TestBase):
    def setUp(self):
        super(TestRavenDB12257, self).setUp()

    def test_can_use_subscription_includes_via_strongly_typed_api(self):
        with self.store.open_session() as session:
            product = Product()
            category = Category()
            supplier = Supplier()

            session.store(category)
            session.store(product)

            product.category = category.Id
            product.supplier = supplier.Id

            session.store(product)

            session.save_changes()

        options = SubscriptionCreationOptions()
        options.includes = lambda builder: builder.include_documents("category").include_documents("supplier")
        name = self.store.subscriptions.create_for_options_autocomplete_query(Product, options)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Product) as sub:
            semaphore = Semaphore(0)

            def _run(batch: SubscriptionBatch[Product]):
                self.assertGreater(len(batch.items), 0)
                with batch.open_session() as s:
                    for item in batch.items:
                        s.load(item.result.category, Category)
                        s.load(item.result.supplier, Supplier)
                        product = s.load(item.key, Product)
                        self.assertEqual(item.result, product)
                        self.assertEqual(0, s.advanced.number_of_requests)
                semaphore.release()

            sub.run(_run)

            semaphore.acquire(timeout=_reasonable_amount_of_time)
