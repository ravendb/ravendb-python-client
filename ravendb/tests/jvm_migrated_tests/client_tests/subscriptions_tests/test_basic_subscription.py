import _queue
import datetime
import queue
import time
import unittest
from threading import Event, Semaphore
from typing import Optional, List

from ravendb.documents.session.event_args import BeforeRequestEventArgs
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionWorkerOptions,
    SubscriptionOpeningStrategy,
    SubscriptionUpdateOptions,
)
from ravendb.documents.subscriptions.worker import SubscriptionBatch, SubscriptionWorker
from ravendb.exceptions.exceptions import (
    SubscriptionInUseException,
    SubscriptionClosedException,
)
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

    def test_will_acknowledge_empty_batches(self):
        subscription_documents = self.store.subscriptions.get_subscriptions(0, 10)
        self.assertEqual(0, len(subscription_documents))

        all_id = self.store.subscriptions.create_for_options_autocomplete_query(User, SubscriptionCreationOptions())
        with self.store.subscriptions.get_subscription_worker_by_name(subscription_name=all_id) as all_subscription:
            all_semaphore = Semaphore(0)
            all_counter = 0

            filtered_options = SubscriptionCreationOptions(query="from 'Users' where age < 0")
            filtered_users_id = self.store.subscriptions.create_for_options(filtered_options)

            with self.store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(filtered_users_id)
            ) as filtered_subscription:
                users_docs_semaphore = Semaphore(0)

                with self.store.open_session() as session:
                    for i in range(500):
                        session.store(User(age=0), f"another/{i}")
                    session.save_changes()

                def __run(x: SubscriptionBatch):
                    nonlocal all_counter
                    all_counter += x.number_of_items_in_batch
                    if all_counter >= 100:
                        all_semaphore.release()

                all_subscription.run(__run)
                filtered_subscription.run(lambda x: users_docs_semaphore.release())

                self.assertTrue(all_semaphore.acquire(timeout=self.reasonable_amount_of_time))
                self.assertFalse(users_docs_semaphore.acquire(timeout=0.05))

    def test_can_update_subscription_by_name(self):
        subscription_creation_options = SubscriptionCreationOptions("Created", "from Users")
        subs_id = self.store.subscriptions.create_for_options(subscription_creation_options)
        subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        state = subscriptions[0]

        self.assertEqual(1, len(subscriptions))
        self.assertEqual("Created", state.subscription_name)
        self.assertEqual("from Users", state.query)

        new_query = "from Users where age > 18"

        subscription_update_options = SubscriptionUpdateOptions(subs_id, new_query)
        self.store.subscriptions.update(subscription_update_options)

        new_subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        new_state = new_subscriptions[0]
        self.assertEqual(1, len(new_subscriptions))
        self.assertEqual(state.subscription_name, new_state.subscription_name)
        self.assertEqual(new_query, new_state.query)
        self.assertEqual(state.subscription_id, new_state.subscription_id)

    def test_can_set_to_ignore_errors(self):
        key = self.store.subscriptions.create_for_options_autocomplete_query(User, SubscriptionCreationOptions())
        opt1 = SubscriptionWorkerOptions(key)
        opt1.ignore_subscriber_errors = True
        with self.store.subscriptions.get_subscription_worker(opt1, User) as subscription:
            docs: List[User] = []
            event = Event()

            self._put_user_doc()
            self._put_user_doc()

            def __run(x: SubscriptionBatch):
                docs.extend([item.result for item in x.items])
                if not event.is_set():
                    event.set()
                raise RuntimeError("Fake error")

            subscription_task = subscription.run(__run)

            event.wait(self.reasonable_amount_of_time)
            self.assertIsNotNone(docs.pop(0))
            self.assertIsNotNone(docs.pop(0))
            subscription.close()
            self.assertIsNone(subscription_task.exception(self.reasonable_amount_of_time))

    def test_can_delete_subscription(self):
        id1 = self.store.subscriptions.create_for_class(User)
        id2 = self.store.subscriptions.create_for_class(User)

        subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        self.assertEqual(2, len(subscriptions))

        # test get_subscription_state as well
        subscription_state = self.store.subscriptions.get_subscription_state(id1)
        self.assertIsNone(subscription_state.change_vector_for_next_batch_starting_point)

        self.store.subscriptions.delete(id1)
        self.store.subscriptions.delete(id2)

        subscriptions = self.store.subscriptions.get_subscriptions(0, 5)

        self.assertEqual(0, len(subscriptions))

    def test_update_subscription_should_return_not_modified(self):
        update_options = SubscriptionUpdateOptions("Created", "from Users")
        self.store.subscriptions.create_for_options(update_options)

        subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        state = subscriptions[0]

        self.assertEqual(1, len(subscriptions))
        self.assertEqual("Created", state.subscription_name)
        self.assertEqual("from Users", state.query)

        self.store.subscriptions.update(update_options)

        new_subscriptions = self.store.subscriptions.get_subscriptions(0, 5)
        new_state = new_subscriptions[0]
        self.assertEqual(1, len(new_subscriptions))
        self.assertEqual(state.subscription_name, new_state.subscription_name)
        self.assertEqual(state.query, new_state.query)
        self.assertEqual(state.subscription_id, new_state.subscription_id)

    def test_should_send_all_new_and_modified_docs(self):
        key = self.store.subscriptions.create_for_class(User)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(key)) as subscription:
            names = queue.Queue()

            with self.store.open_session() as session:
                user = User()
                user.name = "James"
                session.store(user, "users/1")
                session.save_changes()

            subscription.run(lambda batch: [names.put(x.result["name"]) for x in batch.items])

            name = names.get(timeout=self.reasonable_amount_of_time)
            self.assertEqual("James", name)

            with self.store.open_session() as session:
                user = User()
                user.name = "Adam"
                session.store(user, "users/12")
                session.save_changes()

            name = names.get(timeout=self.reasonable_amount_of_time)
            self.assertEqual("Adam", name)

            with self.store.open_session() as session:
                user = User()
                user.name = "David"
                session.store(user, "users/1")
                session.save_changes()

            name = names.get(timeout=self.reasonable_amount_of_time)
            self.assertEqual("David", name)

    def test_disposing_one_subscription_should_not_affect_on_notifications_of_others(self):
        subscription1: Optional[SubscriptionWorker[User]] = None
        subscription2: Optional[SubscriptionWorker[User]] = None

        try:
            id1 = self.store.subscriptions.create_for_options_autocomplete_query(User, SubscriptionCreationOptions())
            id2 = self.store.subscriptions.create_for_options_autocomplete_query(User, SubscriptionCreationOptions())

            with self.store.open_session() as session:
                session.store(User(), "users/1")
                session.store(User(), "users/2")
                session.save_changes()

            subscription1 = self.store.subscriptions.get_subscription_worker_by_name(id1, User)
            items1 = queue.Queue()
            subscription1.run(lambda x: [items1.put(i.result) for i in x.items])

            subscription2 = self.store.subscriptions.get_subscription_worker_by_name(id2, User)
            items2 = queue.Queue()
            subscription2.run(lambda x: [items2.put(i.result) for i in x.items])

            user = items1.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/1", user.Id)

            user = items1.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/2", user.Id)

            user = items2.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/1", user.Id)

            user = items2.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/2", user.Id)

            subscription1.close()

            with self.store.open_session() as session:
                session.store(User(), "users/3")
                session.store(User(), "users/4")
                session.save_changes()

            user = items2.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/3", user.Id)

            user = items2.get()
            self.assertIsNotNone(user)
            self.assertEqual("users/4", user.Id)
        finally:
            subscription1.close()
            subscription2.close()

    def test_dispose_subscription_worker_should_not_throw(self):
        mre = Semaphore(0)
        mre2 = Semaphore(0)

        def __event(brea: BeforeRequestEventArgs):
            if "info/remote-task/tcp?database=" in brea.url:
                mre.release()
                self.assertTrue(mre2.acquire(timeout=self.reasonable_amount_of_time))

        self.store.get_request_executor().add_on_before_request(__event)

        key = self.store.subscriptions.create_for_options_autocomplete_query(Company, SubscriptionCreationOptions())
        worker_options = SubscriptionWorkerOptions(key)
        worker_options.ignore_subscriber_errors = True
        worker_options.strategy = SubscriptionOpeningStrategy.TAKE_OVER
        worker = self.store.subscriptions.get_subscription_worker(worker_options, Company, self.store.database)
        t = worker.run(lambda x: None)
        self.assertTrue(mre.acquire(timeout=self.reasonable_amount_of_time))
        worker.close(False)
        mre2.release()

        time.sleep(5)

        self.assertIsNone(t.exception(self.reasonable_amount_of_time))

    def test_ravenDB_3452_should_stop_pulling_docs_if_released(self):
        key = self.store.subscriptions.create_for_class(User)
        options1 = SubscriptionWorkerOptions(key)
        options1.time_to_wait_before_connection_retry = datetime.timedelta(seconds=1)
        with self.store.subscriptions.get_subscription_worker(options1, User) as subscription:
            with self.store.open_session() as session:
                session.store(User(), "users/1")
                session.store(User(), "users/2")
                session.save_changes()

            docs = queue.Queue()
            subscribe = subscription.run(lambda x: [docs.put(item.result) for item in x.items])

            self.assertIsNotNone(docs.get())
            self.assertIsNotNone(docs.get())
            self.store.subscriptions.drop_connection(key)
            try:
                # this can exit normally or throw on drop connection
                # depending on exactly where the drop happens
                subscribe.result(self.reasonable_amount_of_time)
            except Exception as e:
                self.assertIsInstance(e, SubscriptionClosedException)

            with self.store.open_session() as session:
                session.store(User(), "users/3")
                session.store(User(), "users/4")
                session.save_changes()

        with self.assertRaises(_queue.Empty):
            self.assertIsNone(docs.get(timeout=0.05))
        with self.assertRaises(_queue.Empty):
            self.assertIsNone(docs.get(timeout=0.05))
        self.assertTrue(subscribe.done())

    def test_should_deserialize_the_whole_documents_after_typed_subscription(self):
        key = self.store.subscriptions.create_for_options_autocomplete_query(User, SubscriptionCreationOptions())
        with self.store.subscriptions.get_subscription_worker_by_name(key, User) as subscription:
            users = []

            with self.store.open_session() as session:
                user1 = User(age=31)
                session.store(user1, "users/1")

                user2 = User(age=27)
                session.store(user2, "users/12")

                user3 = User(age=25)
                session.store(user3, "users/3")

                session.save_changes()

            subscription.run(lambda x: users.extend([i.result for i in x.items]))

            i = 0
            while not users:
                i += 1
                time.sleep(0.05)
                if i > 100:
                    break

            user = users.pop(0)
            self.assertIsNotNone(user)
            self.assertEqual("users/1", user.Id)
            self.assertEqual(31, user.age)

            user = users.pop(0)
            self.assertIsNotNone(user)
            self.assertEqual("users/12", user.Id)
            self.assertEqual(27, user.age)

            user = users.pop(0)
            self.assertIsNotNone(user)
            self.assertEqual("users/3", user.Id)
            self.assertEqual(25, user.age)

    @unittest.removeResult  # cause emojis seem to corrupt the whole framework, lol
    def test_can_use_emoji(self):
        with self.store.open_session() as session:
            user1 = User()
            user1.name = "user_\uD83D\uDE21\uD83D\uDE21\uD83E\uDD2C\uD83D\uDE00😡😡🤬😀"
            session.store(user1, "users/1")
            session.save_changes()
            user = session.load("users/1", User)
            self.assertEqual(user.name, "user_\uD83D\uDE21\uD83D\uDE21\uD83E\uDD2C\uD83D\uDE00😡😡🤬😀")

        creation_options = SubscriptionCreationOptions(name="user_\uD83D\uDE21\uD83D\uDE21\uD83E\uDD2C\uD83D\uDE00😡😡🤬😀")
        key = self.store.subscriptions.create_for_options_autocomplete_query(User, creation_options)
        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(key), User) as subscription:
            keys = queue.Queue()
            subscription.run(lambda batch: [keys.put(x.result.name) for x in batch.items])

            key = keys.get(timeout=self.reasonable_amount_of_time)
            self.assertEqual(user1.name, key)

    def test_can_disable_subscription_via_api(self):
        subscription = self.store.subscriptions.create_for_class(User)
        self.store.subscriptions.disable(subscription)

        subscriptions = self.store.subscriptions.get_subscriptions(0, 10)
        self.assertTrue(subscriptions[0].disabled)

        self.store.subscriptions.enable(subscription)
        subscriptions = self.store.subscriptions.get_subscriptions(0, 10)
        self.assertFalse(subscriptions[0].disabled)

    def test_should_throw_on_attempt_to_open_already_opened_subscription(self):
        key = self.store.subscriptions.create_for_class(User)
        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(key)) as subscription:
            with self.store.open_session() as session:
                session.store(User())
                session.save_changes()

            semaphore = Semaphore(0)
            t = subscription.run(lambda x: semaphore.release())

            semaphore.acquire(timeout=self.reasonable_amount_of_time)

            options2 = SubscriptionWorkerOptions(key)
            options2.strategy = SubscriptionOpeningStrategy.OPEN_IF_FREE
            with self.store.subscriptions.get_subscription_worker(options2) as second_subscription:
                with self.assertRaises(SubscriptionInUseException):
                    task = second_subscription.run(lambda x: None)
                    task.result(self.reasonable_amount_of_time)
