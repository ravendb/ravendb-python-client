import time
import unittest
from threading import Event
from typing import List

from ravendb import DocumentStore
from ravendb.documents.subscriptions.options import SubscriptionWorkerOptions
from ravendb.documents.subscriptions.worker import SubscriptionWorker
from ravendb.documents.subscriptions.worker_status import SubscriptionWorkerState, SubscriptionWorkerStatus
from ravendb.exceptions.exceptions import SubscriptionInUseException
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB27175(TestBase):
    def setUp(self):
        super(TestRavenDB27175, self).setUp()
        self.reasonable_wait_time = 60

    def _wait_for_state(self, worker: SubscriptionWorker, state: SubscriptionWorkerState) -> SubscriptionWorkerState:
        deadline = time.monotonic() + self.reasonable_wait_time
        while time.monotonic() < deadline:
            if worker.status.state == state:
                break
            time.sleep(0.025)
        return worker.status.state

    def test_can_be_notified_on_established_connection_of_an_idle_subscription(self):
        name = self.store.subscriptions.create_for_class(Company)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Company) as worker:
            connected = Event()
            state_when_connected: List[SubscriptionWorkerState] = []

            def _on_established():
                state_when_connected.append(worker.status.state)
                connected.set()

            worker.add_on_established_subscription_connection(_on_established)

            # no documents are ever stored here, so the event is the only indication that the worker is connected
            worker.run(lambda batch: None)

            self.assertTrue(connected.wait(self.reasonable_wait_time), "initial connection wasn't established")
            self.assertEqual(SubscriptionWorkerState.WAITING_FOR_DOCUMENTS, state_when_connected[0])

    def test_status_follows_the_worker_from_connecting_through_processing(self):
        with self.store.open_session() as session:
            session.store(Company(name="RavenDB"))
            session.save_changes()

        name = self.store.subscriptions.create_for_class(Company)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Company) as worker:
            self.assertEqual(SubscriptionWorkerState.NOT_STARTED, worker.status.state)
            self.assertIsNone(worker.status.exception)

            observed: List[SubscriptionWorkerState] = []
            raised_by: List[object] = []

            def _on_state_changed(sender, status: SubscriptionWorkerStatus):
                raised_by.append(sender)
                observed.append(status.state)

            worker.add_on_state_changed(_on_state_changed)

            processed = Event()
            state_while_processing: List[SubscriptionWorkerState] = []

            def _process(batch):
                state_while_processing.append(worker.status.state)
                processed.set()

            worker.run(_process)

            self.assertTrue(processed.wait(self.reasonable_wait_time), "the batch was never handed to the subscriber")
            self.assertEqual(SubscriptionWorkerState.PROCESSING, state_while_processing[0])

            # once the subscriber returned and the batch was acknowledged, the worker is idle again
            self.assertEqual(
                SubscriptionWorkerState.WAITING_FOR_DOCUMENTS,
                self._wait_for_state(worker, SubscriptionWorkerState.WAITING_FOR_DOCUMENTS),
            )

            self.assertEqual(
                [
                    SubscriptionWorkerState.CONNECTING,
                    SubscriptionWorkerState.WAITING_FOR_DOCUMENTS,
                    SubscriptionWorkerState.PROCESSING,
                ],
                observed[:3],
            )

            self.assertTrue(raised_by)
            for sender in raised_by:
                self.assertIs(worker, sender)

    def test_status_is_faulted_with_the_failure_when_the_worker_gives_up(self):
        name = self.store.subscriptions.create_for_class(Company)

        with self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Company) as worker:
            connected = Event()
            worker.add_on_established_subscription_connection(connected.set)
            worker.run(lambda batch: None)
            self.assertTrue(connected.wait(self.reasonable_wait_time), "initial connection wasn't established")

            # the subscription is taken, which is a failure the second worker cannot recover from by reconnecting
            with self.store.subscriptions.get_subscription_worker(
                SubscriptionWorkerOptions(name), Company
            ) as second_worker:
                task = second_worker.run(lambda batch: None)

                with self.assertRaises(SubscriptionInUseException) as ctx:
                    task.result(self.reasonable_wait_time)

                self.assertEqual(SubscriptionWorkerState.FAULTED, second_worker.status.state)
                self.assertIs(ctx.exception, second_worker.status.exception)
                self.assertIsNotNone(second_worker.status.failing_since_utc)

    def test_status_is_stopped_after_close(self):
        name = self.store.subscriptions.create_for_class(Company)

        worker = self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Company)
        connected = Event()
        worker.add_on_established_subscription_connection(connected.set)

        worker.run(lambda batch: None)

        self.assertTrue(connected.wait(self.reasonable_wait_time), "initial connection wasn't established")

        worker.close()

        self.assertEqual(SubscriptionWorkerState.STOPPED, worker.status.state)
        self.assertIsNone(worker.status.exception)

    def test_status_is_stopped_after_closing_a_worker_that_was_never_run(self):
        name = self.store.subscriptions.create_for_class(Company)

        worker = self.store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(name), Company)

        self.assertEqual(SubscriptionWorkerState.NOT_STARTED, worker.status.state)

        worker.close()

        self.assertEqual(SubscriptionWorkerState.STOPPED, worker.status.state)


class TestSubscriptionWorkerStatusTransitions(unittest.TestCase):
    """
    Covers the state bookkeeping of the worker without a server: the transitions are driven directly.
    """

    def setUp(self):
        self.store = DocumentStore(["http://127.0.0.1:8080"], "db")
        self.worker = SubscriptionWorker(Company, SubscriptionWorkerOptions("subscription"), False, self.store, None)
        self.changes: List[SubscriptionWorkerStatus] = []
        self.worker.add_on_state_changed(lambda sender, status: self.changes.append(status))

    def test_new_worker_is_not_started(self):
        worker = SubscriptionWorker(Company, SubscriptionWorkerOptions("subscription"), False, self.store, None)

        self.assertEqual(SubscriptionWorkerState.NOT_STARTED, worker.status.state)
        self.assertIsNone(worker.status.exception)
        self.assertIsNone(worker.status.failing_since_utc)
        self.assertIsNotNone(worker.status.since_utc)

    def test_closing_a_worker_that_was_never_run_reports_stopped(self):
        senders = []
        self.worker.add_on_state_changed(lambda sender, status: senders.append(sender))

        self.worker.close()

        self.assertEqual(SubscriptionWorkerState.STOPPED, self.worker.status.state)
        self.assertEqual([SubscriptionWorkerState.STOPPED], [s.state for s in self.changes])
        self.assertEqual([self.worker], senders)

    def test_failing_since_utc_survives_the_retry_cycle_and_is_cleared_on_recovery(self):
        self.worker._set_state(SubscriptionWorkerState.CONNECTING)
        self.worker._set_state(SubscriptionWorkerState.WAITING_FOR_DOCUMENTS)
        self.assertIsNone(self.worker.status.failing_since_utc)

        first_failure = RuntimeError("first")
        self.worker._set_state(SubscriptionWorkerState.RETRYING, first_failure)
        first = self.worker.status

        self.assertIs(first_failure, first.exception)
        self.assertIsNotNone(first.failing_since_utc)

        time.sleep(0.01)

        # CONNECTING neither starts nor ends the failing stretch, it carries it over
        self.worker._set_state(SubscriptionWorkerState.CONNECTING)
        self.assertEqual(first.failing_since_utc, self.worker.status.failing_since_utc)
        self.assertIsNone(self.worker.status.exception)

        second_failure = RuntimeError("second")
        self.worker._set_state(SubscriptionWorkerState.RETRYING, second_failure)
        later = self.worker.status

        self.assertGreater(later.since_utc, first.since_utc)
        self.assertEqual(first.failing_since_utc, later.failing_since_utc)
        self.assertIs(second_failure, later.exception)

        self.worker._set_state(SubscriptionWorkerState.CONNECTING)
        self.assertEqual(first.failing_since_utc, self.worker.status.failing_since_utc)

        # reaching the server clears it
        self.worker._set_state(SubscriptionWorkerState.WAITING_FOR_DOCUMENTS)
        self.assertIsNone(self.worker.status.failing_since_utc)
        self.assertIsNone(self.worker.status.exception)

    def test_reaching_the_same_state_again_does_not_raise_the_event(self):
        self.worker._set_state(SubscriptionWorkerState.PROCESSING)
        since = self.worker.status.since_utc

        self.worker._set_state(SubscriptionWorkerState.PROCESSING)

        self.assertEqual([SubscriptionWorkerState.PROCESSING], [s.state for s in self.changes])
        self.assertEqual(since, self.worker.status.since_utc)

    def test_a_failing_state_changed_handler_does_not_break_the_worker(self):
        def _failing_handler(sender, status):
            raise ValueError("handler failure")

        self.worker.remove_on_state_changed(self.worker._on_state_changed[0])
        self.worker.add_on_state_changed(_failing_handler)
        self.worker.add_on_state_changed(lambda sender, status: self.changes.append(status))

        self.worker._set_state(SubscriptionWorkerState.CONNECTING)

        self.assertEqual(SubscriptionWorkerState.CONNECTING, self.worker.status.state)
        self.assertEqual([SubscriptionWorkerState.CONNECTING], [s.state for s in self.changes])

    def test_established_connection_handlers_can_be_added_and_removed(self):
        calls = []

        def _handler():
            calls.append(1)

        self.worker.add_on_established_subscription_connection(_handler)
        self.worker.invoke_on_established_subscription_connection()
        self.worker.remove_on_established_subscription_connection(_handler)
        self.worker.invoke_on_established_subscription_connection()

        self.assertEqual([1], calls)

    def test_status_text_contains_the_state_the_failing_time_and_the_failure(self):
        self.worker._set_state(SubscriptionWorkerState.RETRYING, RuntimeError("connection lost"))
        status = self.worker.status

        text = str(status)

        self.assertTrue(text.startswith(f"Retrying since {status.since_utc.isoformat()}"))
        self.assertIn(f", failing since {status.failing_since_utc.isoformat()}", text)
        self.assertTrue(text.endswith(" because of RuntimeError: connection lost"))


if __name__ == "__main__":
    unittest.main()
