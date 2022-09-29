from threading import Event, Semaphore
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionWorkerOptions,
    SubscriptionOpeningStrategy,
    SubscriptionUpdateOptions,
)
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.exceptions.exceptions import SubscriptionInUseException, SubscriptionDoesNotExistException
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestBasicSubscription(TestBase):
    def setUp(self):
        super(TestBasicSubscription, self).setUp()
        self.reasonable_amount_of_time = 60

    def _put_user_doc(self):
        with self.store.open_session() as session:
            session.store(User())
            session.save_changes()

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

    def test_should_respect_max_doc_count_in_batch(self):
        with self.store.open_session() as session:
            for i in range(100):
                session.store(Company())

            session.save_changes()

        key = self.store.subscriptions.create_for_class(Company)
        options = SubscriptionWorkerOptions(key)
        options.max_docs_per_batch = 25

        with self.store.subscriptions.get_subscription_worker(options) as subscription_worker:
            semaphore = Semaphore(0)
            total_items = 0

            def __run(batch: SubscriptionBatch):
                nonlocal total_items
                total_items += batch.number_of_items_in_batch
                self.assertLessEqual(batch.number_of_items_in_batch, 25)

                if total_items == 100:
                    semaphore.release()

            subscription_worker.run(__run)
            self.assertTrue(semaphore.acquire(True, self.reasonable_amount_of_time))

    def test_update_non_existent_subscription_should_throw(self):
        name = "Update"
        key = 322

        with self.assertRaises(RuntimeError):  # todo: Change for SubscriptionDoesNotExistException
            subscription_update_options = SubscriptionUpdateOptions(name)
            self.store.subscriptions.update(subscription_update_options)

        with self.assertRaises(RuntimeError):  # todo: Change for SubscriptionDoesNotExistException
            subscription_update_options = SubscriptionUpdateOptions(name, key=key)
            self.store.subscriptions.update(subscription_update_options)

        subscription_creation_options = SubscriptionCreationOptions("Created", "from Users")
        subs_id = self.store.subscriptions.create_for_options(subscription_creation_options)

        with self.assertRaises(RuntimeError):  # todo: Change for SubscriptionDoesNotExistException
            subscription_update_options = SubscriptionUpdateOptions(subs_id, key=key)
            self.store.subscriptions.update(subscription_update_options)

    def test_should_stream_all_documents_after_subscription_creation(self):
        with self.store.open_session() as session:
            user1 = User(age=31)
            session.store(user1, "users/1")

            user2 = User(age=27)
            session.store(user2, "users/12")

            user3 = User(age=25)
            session.store(user3, "users/3")

            session.save_changes()

        key = self.store.subscriptions.create_for_class(User)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(key), User) as subscription:
            keys = []
            ages = []
            event = Event()

            def __run(batch: SubscriptionBatch):
                nonlocal keys
                nonlocal ages
                for item in batch.items:
                    keys.append(item.key)
                    ages.append(item.result.age)
                event.set()

            subscription.run(__run)
            event.wait(self.reasonable_amount_of_time)
            self.assertEqual("users/1", keys[0])
            self.assertEqual("users/12", keys[1])
            self.assertEqual("users/3", keys[2])

            self.assertEqual(31, ages[0])
            self.assertEqual(27, ages[1])
            self.assertEqual(25, ages[2])

    def test_can_update_subscription_by_id(self):
        subscription_creation_options = SubscriptionCreationOptions("Created", "from Users")
        self.store.subscriptions.create_for_options(subscription_creation_options)
        subscriptions = self.store.subscriptions.get_subscriptions(0, 5)

        state = subscriptions[0]

        self.assertEqual(1, len(subscriptions))
        self.assertEqual("Created", state.subscription_name)
        self.assertEqual("from Users", state.query)

        new_query = "from Users where age > 18"

        subscription_update_options = SubscriptionUpdateOptions(key=state.subscription_id, query=new_query)
        self.store.subscriptions.update(subscription_update_options)

        new_subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        new_state = new_subscriptions[0]
        self.assertEqual(1, len(new_subscriptions))
        self.assertEqual(state.subscription_name, new_state.subscription_name)
        self.assertEqual(new_query, new_state.query)
        self.assertEqual(state.subscription_id, new_state.subscription_id)

    def test_should_respect_collection_criteria(self):
        with self.store.open_session() as session:
            for i in range(100):
                session.store(Company())
                session.store(User())

            session.save_changes()

        key = self.store.subscriptions.create_for_class(User)

        options = SubscriptionWorkerOptions(key)
        options.max_docs_per_batch = 31
        with self.store.subscriptions.get_subscription_worker(options) as subscription:
            semaphore = Semaphore(0)
            atomic_integer = 0

            def __run(batch: SubscriptionBatch):
                nonlocal atomic_integer
                atomic_integer += batch.number_of_items_in_batch

                if atomic_integer == 100:
                    semaphore.release()

            subscription.run(__run)
            self.assertTrue(semaphore.acquire(timeout=self.reasonable_amount_of_time))
