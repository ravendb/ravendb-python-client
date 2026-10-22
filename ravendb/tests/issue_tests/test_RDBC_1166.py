import concurrent.futures
import socket
import threading
import time
import unittest
from datetime import timedelta
from threading import Event
from typing import List

from ravendb import DocumentStore
from ravendb.documents.subscriptions.options import SubscriptionWorkerOptions
from ravendb.documents.subscriptions.worker import (
    BatchFromServer,
    SubscriptionConnectionServerMessage,
    SubscriptionWorker,
)
from ravendb.documents.subscriptions.worker_status import SubscriptionWorkerState
from ravendb.infrastructure.orders import Company
from ravendb.primitives.exceptions import OperationCancelledException
from ravendb.tests.test_base import TestBase


class TestRDBC1166(TestBase):
    def setUp(self):
        super(TestRDBC1166, self).setUp()
        self.reasonable_wait_time = 60

    def test_idle_subscription_stays_connected(self):
        name = self.store.subscriptions.create_for_class(Company)

        options = SubscriptionWorkerOptions(name)
        # a worker that took a wait without documents for a failure would reconnect after every interval
        options.time_to_wait_before_connection_retry = timedelta(seconds=1)

        with self.store.subscriptions.get_subscription_worker(options, Company) as worker:
            connections: List[SubscriptionWorkerState] = []
            retries: List[Exception] = []
            errors: List[Exception] = []
            states: List[SubscriptionWorkerState] = []
            connected = Event()
            processed = Event()

            def _on_established():
                connections.append(worker.status.state)
                connected.set()

            worker.add_on_established_subscription_connection(_on_established)
            worker.add_on_subscription_connection_retry(retries.append)
            worker.add_on_unexpected_subscription_error(errors.append)
            worker.add_on_state_changed(lambda sender, status: states.append(status.state))

            worker.run(lambda batch: processed.set())

            self.assertTrue(connected.wait(self.reasonable_wait_time), "initial connection wasn't established")

            # several retry intervals and server heartbeats pass without a document to send
            time.sleep(6)

            self.assertEqual(SubscriptionWorkerState.WAITING_FOR_DOCUMENTS, worker.status.state)
            self.assertIsNone(worker.status.exception)
            self.assertIsNone(worker.status.failing_since_utc)
            self.assertEqual(
                [SubscriptionWorkerState.CONNECTING, SubscriptionWorkerState.WAITING_FOR_DOCUMENTS], states
            )
            self.assertEqual([SubscriptionWorkerState.WAITING_FOR_DOCUMENTS], connections)
            self.assertEqual([], retries)
            self.assertEqual([], errors)

            # the connection that was kept still delivers documents
            with self.store.open_session() as session:
                session.store(Company(name="RavenDB"))
                session.save_changes()

            self.assertTrue(processed.wait(self.reasonable_wait_time), "the batch was never handed to the subscriber")
            self.assertEqual(1, len(connections))
            self.assertEqual([], retries)

    def test_connection_uses_the_connection_stream_timeout(self):
        name = self.store.subscriptions.create_for_class(Company)

        options = SubscriptionWorkerOptions(name, connection_stream_timeout=timedelta(seconds=20))

        with self.store.subscriptions.get_subscription_worker(options, Company) as worker:
            connected = Event()
            timeouts: List[float] = []

            def _on_established():
                timeouts.append(worker._tcp_client.gettimeout())
                connected.set()

            worker.add_on_established_subscription_connection(_on_established)
            worker.run(lambda batch: None)

            self.assertTrue(connected.wait(self.reasonable_wait_time), "initial connection wasn't established")
            self.assertEqual([20], timeouts)


class TestSubscriptionWorkerWaitForBatch(unittest.TestCase):
    """
    Covers how the worker waits for and reads the next batch without a server.
    """

    def setUp(self):
        self.store = DocumentStore(["http://127.0.0.1:8080"], "db")
        options = SubscriptionWorkerOptions("subscription")
        options.time_to_wait_before_connection_retry = timedelta(milliseconds=100)
        self.worker = SubscriptionWorker(Company, options, False, self.store, None)
        self.worker._CLOSE_CHECK_INTERVAL_IN_SECONDS = 0.05

    def _complete_later(self, action) -> None:
        timer = threading.Timer(0.5, action)
        timer.start()
        self.addCleanup(timer.cancel)

    def test_waits_for_a_batch_longer_than_the_retry_interval(self):
        read_from_server = concurrent.futures.Future()
        batch = BatchFromServer()
        self._complete_later(lambda: read_from_server.set_result(batch))

        self.assertIs(batch, self.worker._wait_for_batch_from_server(read_from_server))

    def test_stops_waiting_for_a_batch_when_the_worker_is_closed(self):
        read_from_server = concurrent.futures.Future()
        self._complete_later(self.worker._processing_cts.cancel)

        with self.assertRaises(OperationCancelledException):
            self.worker._wait_for_batch_from_server(read_from_server)

    def test_a_failed_read_ends_the_wait_with_its_failure(self):
        read_from_server = concurrent.futures.Future()
        failure = ConnectionError("connection lost")
        self._complete_later(lambda: read_from_server.set_exception(failure))

        with self.assertRaises(ConnectionError) as ctx:
            self.worker._wait_for_batch_from_server(read_from_server)

        self.assertIs(failure, ctx.exception)

    def _read_next_object_on_another_thread(self, sock: socket.socket):
        # a read that never ends fails the test instead of hanging it
        outcome = []

        def _read():
            try:
                outcome.append(self.worker._read_next_object(sock))
            except Exception as e:
                outcome.append(e)

        reader = threading.Thread(target=_read, daemon=True)
        reader.start()
        reader.join(10)

        self.assertFalse(reader.is_alive(), "the read never ended")
        return outcome[0]

    def test_reads_the_next_message_after_heartbeats(self):
        client, server = socket.socketpair()
        self.addCleanup(client.close)
        self.addCleanup(server.close)

        server.sendall(b'\r\n\r\n{"Type": "EndOfBatch"}')

        message = self._read_next_object_on_another_thread(client)

        self.assertEqual(SubscriptionConnectionServerMessage.MessageType.END_OF_BATCH, message.type_of_message)

    def test_reading_from_a_connection_closed_by_the_server_raises(self):
        client, server = socket.socketpair()
        self.addCleanup(client.close)

        server.sendall(b"\r\n")
        server.close()

        self.assertIsInstance(self._read_next_object_on_another_thread(client), ConnectionError)


class TestSubscriptionWorkerOptionsConnectionStreamTimeout(unittest.TestCase):
    def test_defaults_to_30_seconds(self):
        self.assertEqual(timedelta(seconds=30), SubscriptionWorkerOptions("subscription").connection_stream_timeout)

    def test_cannot_be_smaller_than_15_seconds(self):
        with self.assertRaises(ValueError):
            SubscriptionWorkerOptions("subscription", connection_stream_timeout=timedelta(seconds=14))

        options = SubscriptionWorkerOptions("subscription")
        options.connection_stream_timeout = timedelta(seconds=15)
        self.assertEqual(timedelta(seconds=15), options.connection_stream_timeout)

        with self.assertRaises(ValueError):
            options.connection_stream_timeout = timedelta(seconds=1)


if __name__ == "__main__":
    unittest.main()
