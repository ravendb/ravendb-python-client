import threading
from functools import partial
from pyravendb.subscriptions.data import *
from pyravendb.commands.raven_commands import GetSubscriptionsCommand
from pyravendb.tests.test_base import TestBase
from pyravendb.custom_exceptions.exceptions import SubscriptionInUseException, SubscriptionClosedException
import unittest


class User:
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name

    def __str__(self):
        return self.first_name + " " + self.last_name


class TestSubscription(TestBase):
    def setUp(self):
        super(TestSubscription, self).setUp()
        self.results = []
        self.items_count = 0
        self.expected_items_count = None
        self.event = threading.Event()
        self.ack = threading.Event()

    def process_documents(self, batch):
        self.items_count += len(batch.items)
        for b in batch.items:
            self.results.append(b.result)
        if self.items_count == self.expected_items_count:
            self.event.set()

    def acknowledge(self, batch):
        if not self.ack.is_set():
            self.ack.set()

    def tearDown(self):
        super(TestSubscription, self).tearDown()
        self.delete_all_topology_files()
        self.event.clear()
        self.ack.clear()

    def test_create_and_run_subscription_with_object(self):
        self.expected_items_count = 2
        with self.store.open_session() as session:
            session.store(User("Idan", "Shalom"))
            session.store(User("Ilay", "Shalom"))
            session.store(User("Raven", "DB"))
            session.save_changes()

        creation_options = SubscriptionCreationOptions("FROM Users where last_name='Shalom'")
        self.store.subscriptions.create(creation_options)

        request_executor = self.store.get_request_executor()
        subscriptions = request_executor.execute(GetSubscriptionsCommand(0, 1))
        self.assertGreaterEqual(len(subscriptions), 1)

        connection_options = SubscriptionWorkerOptions(subscriptions[0].subscription_name)
        self.assertEqual(len(self.results), 0)
        with self.store.subscriptions.get_subscription_worker(connection_options, object_type=User) as subscription:
            subscription.run(self.process_documents)
            self.event.wait(timeout=5)

        self.assertEqual(len(self.results), 2)
        for item in self.results:
            self.assertTrue(isinstance(item, User))

    def test_subscription_continue_to_take_documents(self):
        self.expected_items_count = 3
        with self.store.open_session() as session:
            session.store(User("Idan", "Shalom"))
            session.store(User("Ilay", "Shalom"))
            session.store(User("Raven", "DB"))
            session.save_changes()

        self.store.subscriptions.create("FROM Users where last_name='Shalom'")

        request_executor = self.store.get_request_executor()
        subscriptions = request_executor.execute(GetSubscriptionsCommand(0, 1))
        self.assertGreaterEqual(len(subscriptions), 1)

        connection_options = SubscriptionWorkerOptions(subscriptions[0].subscription_name)
        self.assertEqual(len(self.results), 0)
        with self.store.subscriptions.get_subscription_worker(connection_options) as subscription:
            subscription.confirm_callback = self.acknowledge
            subscription.run(self.process_documents)
            self.ack.wait(timeout=5)

            self.assertEqual(len(self.results), 2)
            with self.store.open_session() as session:
                session.store(User("Idan", "Shalom"))
                session.save_changes()
            self.event.wait(timeout=5)
            self.assertEqual(len(self.results), 3)

    def test_drop_subscription(self):
        self.expected_items_count = 2
        with self.store.open_session() as session:
            session.store(User("Idan", "Shalom"))
            session.store(User("Ilay", "Shalom"))
            session.store(User("Raven", "DB"))
            session.save_changes()
        try:
            creation_options = SubscriptionCreationOptions("FROM Users where last_name='Shalom'")
            name = self.store.subscriptions.create(creation_options)

            worker_options = SubscriptionWorkerOptions(name, strategy=SubscriptionOpeningStrategy.wait_for_free)
            self.assertEqual(len(self.results), 0)
            subscription = self.store.subscriptions.get_subscription_worker(worker_options, object_type=User)
            subscription.run(self.process_documents)
            self.event.wait()

            subscription_throw = self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name),
                                                                                  object_type=User)

            th = subscription_throw.run(self.process_documents)
            with self.assertRaises(SubscriptionInUseException):
                th.join()

            self.items_count = 0
            self.event.clear()
            self.store.subscriptions.drop_connection(name)
            with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name),
                                                                  object_type=User) as subscription:
                sub = subscription.run(self.process_documents)
                with self.store.open_session() as session:
                    session.store(User("Idan", "Shalom"))
                    session.store(User("Ilay", "Shalom"))
                    session.save_changes()

                self.event.wait()

            self.assertEqual(len(self.results), 4)
            for item in self.results:
                self.assertTrue(isinstance(item, User))

        finally:
            subscription.close()
            subscription_throw.close()

    def test_subscription_with_close_when_no_docs_left(self):
        with self.store.open_session() as session:
            for _ in range(0, 5000):
                session.store(User("Idan", "Shalom"))
                session.store(User("Ilay", "Shalom"))
            session.save_changes()

        users = []
        subscription_creation_options = SubscriptionCreationOptions("From Users")
        subscription_name = self.store.subscriptions.create(subscription_creation_options)

        worker_options = SubscriptionWorkerOptions(subscription_name,
                                                   strategy=SubscriptionOpeningStrategy.take_over,
                                                   close_when_no_docs_left=True)
        with self.store.subscriptions.get_subscription_worker(worker_options) as subscription_worker:
            try:
                subscription_worker.run(
                    partial(lambda batch, x=users: x.extend([item.raw_result for item in batch.items]))).join()
            except SubscriptionClosedException:
                # That's expected
                pass

        self.assertEqual(len(users), 10000)


if __name__ == "__main__":
    unittest.main()
