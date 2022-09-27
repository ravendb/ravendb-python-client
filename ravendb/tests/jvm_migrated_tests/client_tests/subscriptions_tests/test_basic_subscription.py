from threading import Event, Semaphore
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionWorkerOptions,
    SubscriptionOpeningStrategy,
)
from ravendb.exceptions.exceptions import SubscriptionInUseException
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestBasicSubscription(TestBase):
    def setUp(self):
        super(TestBasicSubscription, self).setUp()
        self.reasonable_amount_of_time = 60

    def test_can_release_subscription(self):
        subscription_worker = None
        throwing_subscription_worker = None
        not_throwing_subscription_worker = None

        try:
            key = self.store.subscriptions.create_for_class(User, SubscriptionCreationOptions())
            options1 = SubscriptionWorkerOptions(key)
            options1.strategy = SubscriptionOpeningStrategy.OPEN_IF_FREE
            subscription_worker = self.store.subscriptions.get_subscription_worker(options1, User)

            event = Event()
            self._put_user_doc()

            subscription_worker.run(lambda x: event.set())
            event.wait(self.reasonable_amount_of_time)
            self.assertTrue(event.is_set())

            options2 = SubscriptionWorkerOptions(key)
            throwing_subscription_worker = self.store.subscriptions.get_subscription_worker(options2)
            subscription_task = throwing_subscription_worker.run(lambda x: None)

            with self.assertRaises(SubscriptionInUseException):
                subscription_task.result()

            event.clear()

            self.store.subscriptions.drop_connection(key)
            not_throwing_subscription_worker = self.store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(key)
            )
            t = not_throwing_subscription_worker.run(lambda x: event.set())
            self._put_user_doc()

            event.wait(self.reasonable_amount_of_time)
            self.assertTrue(event.is_set())
        finally:
            if subscription_worker is not None:
                subscription_worker.close()
            if throwing_subscription_worker is not None:
                throwing_subscription_worker.close()
            if not_throwing_subscription_worker is not None:
                not_throwing_subscription_worker.close()

    def _put_user_doc(self):
        with self.store.open_session() as session:
            session.store(User())
            session.save_changes()
