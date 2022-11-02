from queue import Queue
from ravendb.documents.subscriptions.options import SubscriptionWorkerOptions
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase

_reasonable_amount_of_time = 4


class TestSecuredSubscriptionBasic(TestBase):
    def setUp(self):
        super(TestSecuredSubscriptionBasic, self).setUp()

    def test_should_stream_all_documents_after_subscription_creation(self):
        with self.secured_document_store.open_session() as session:
            user1 = User("users/1", age=31)
            user2 = User("users/12", age=27)
            user3 = User("users/3", age=25)
            session.store(user1)
            session.store(user2)
            session.store(user3)
            session.save_changes()

        key = self.secured_document_store.subscriptions.create_for_class(User)

        with self.secured_document_store.subscriptions.get_subscription_worker(
            SubscriptionWorkerOptions(key), User
        ) as subscription:
            keys = Queue()
            ages = Queue()

            def _run(batch: SubscriptionBatch[User]):
                for item in batch.items:
                    keys.put(item.result.Id)
                    ages.put(item.result.age)

            subscription.run(_run)

            key = keys.get(timeout=_reasonable_amount_of_time)
            self.assertIsNotNone(key)
            self.assertEqual("users/1", key)

            key = keys.get(timeout=_reasonable_amount_of_time)
            self.assertIsNotNone(key)
            self.assertEqual("users/12", key)

            key = keys.get(timeout=_reasonable_amount_of_time)
            self.assertIsNotNone(key)
            self.assertEqual("users/3", key)

            age = ages.get(timeout=_reasonable_amount_of_time)
            self.assertEqual(age, 31)

            age = ages.get(timeout=_reasonable_amount_of_time)
            self.assertEqual(age, 27)

            age = ages.get(timeout=_reasonable_amount_of_time)
            self.assertEqual(age, 25)

    def test_should_send_all_new_and_modified_docs(self):
        with self.secured_document_store as store:
            key = store.subscriptions.create_for_class(User)

            with store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(key), User) as subscription:
                names = Queue(20)

                with store.open_session() as session:
                    user = User(name="James")
                    session.store(user, "users/1")
                    session.save_changes()

                subscription.run(lambda batch: [names.put(item.result.name) for item in batch.items])

                name = names.get(timeout=_reasonable_amount_of_time)
                self.assertEqual("James", name)

                with store.open_session() as session:
                    user = User(name="Adam")
                    session.store(user, "users/12")
                    session.save_changes()

                name = names.get(timeout=_reasonable_amount_of_time)
                self.assertEqual("Adam", name)

                with store.open_session() as session:
                    user = User(name="David")
                    session.store(user, "users/1")
                    session.save_changes()

                name = names.get(timeout=_reasonable_amount_of_time)
                self.assertEqual("David", name)
