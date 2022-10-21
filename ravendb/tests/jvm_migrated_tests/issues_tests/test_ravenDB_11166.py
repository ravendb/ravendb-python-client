from threading import Semaphore

from ravendb.documents.subscriptions.options import SubscriptionCreationOptions
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.tests.test_base import TestBase

_reasonable_amount_of_time = 15


class Person:
    def __init__(self, name: str):
        self.name = name


class Dog:
    def __init__(self, name: str, owner: str):
        self.name = name
        self.owner = owner


class TestRavenDB11166(TestBase):
    def setUp(self):
        super(TestRavenDB11166, self).setUp()

    def test_can_use_subscription_with_includes(self):
        with self.store.open_session() as session:
            person = Person("Arava")
            session.store(person, "people/1")

            dog = Dog("Oscar", "people/1")
            session.store(dog)

            session.save_changes()

        options = SubscriptionCreationOptions(query="from Dogs include owner")
        key = self.store.subscriptions.create_for_options(options)

        with self.store.subscriptions.get_subscription_worker_by_name(key, Dog) as sub:
            semaphore = Semaphore(0)

            def _run(batch: SubscriptionBatch[Dog]):
                self.assertGreater(len(batch.items), 0)
                with batch.open_session() as s:
                    for item in batch.items:
                        s.load(item.result.owner)
                        dog = s.load(item.key, Dog)
                        self.assertEqual(item.result, dog)
                        self.assertEqual(0, s.advanced.number_of_requests)
                semaphore.release()

            run = sub.run(_run)
            self.assertTrue(semaphore.acquire(timeout=15))
