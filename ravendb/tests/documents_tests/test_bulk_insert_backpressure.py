import threading
import time
import unittest
from concurrent.futures import Future, ThreadPoolExecutor

from ravendb.documents.bulk_insert_operation import BulkInsertOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.exceptions.documents.bulkinsert import BulkInsertAbortedException


class Document:
    def __init__(self, Id: str, payload: str):
        self.Id = Id
        self.payload = payload


class _FakeRequestExecutor:
    def __init__(self, conventions: DocumentConventions):
        self.conventions = conventions

    def execute_command(self, command, session_info=None) -> None:
        command.result = {"Status": "Running"}


class _FakeStore:
    """Enough of a document store to drive the bulk insert buffering, with no server behind it."""

    def __init__(self):
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=2)
        self.conventions = DocumentConventions()

    def get_request_executor(self, database: str) -> _FakeRequestExecutor:
        return _FakeRequestExecutor(self.conventions)


class TestBulkInsertBackpressure(unittest.TestCase):
    """A server that reads slower than the caller writes has to slow the caller down, not be buffered."""

    DOCUMENT_COUNT = 200
    PAYLOAD = "x" * 256 * 1024

    def _bulk_insert_with_a_stalled_stream(self) -> BulkInsertOperation:
        bulk = BulkInsertOperation("db", _FakeStore(), None)
        bulk._operation_id = 1
        bulk._ongoing_bulk_insert_execute_task = Future()
        bulk._current_data_buffer += bytearray("[", encoding="utf-8")
        return bulk

    def test_a_stalled_stream_stops_the_caller_instead_of_growing_memory(self):
        bulk = self._bulk_insert_with_a_stalled_stream()
        queue = bulk._buffer_exposer._buffers_to_flush_queue
        self.assertGreater(queue.maxsize, 0, "the outbound queue is unbounded")
        failures, stored = [], []

        def store_documents():
            try:
                for i in range(self.DOCUMENT_COUNT):
                    bulk.store(Document(f"documents/{i}", self.PAYLOAD))
                    stored.append(i)
            except BaseException as e:  # noqa: BLE001 - reported through the assertions below
                failures.append(e)

        producer = threading.Thread(target=store_documents, daemon=True)
        producer.start()

        deadline = time.time() + 30
        while not queue.full() and time.time() < deadline:
            time.sleep(0.01)

        self.assertTrue(queue.full(), "the queue never filled up, so nothing was throttled")
        self.assertLess(len(stored), self.DOCUMENT_COUNT)
        buffered = sum(len(chunk) for chunk in list(queue.queue))
        self.assertLessEqual(buffered, (queue.maxsize + 1) * bulk._max_size_in_buffer)

        # keep it full for several waiting periods: the caller waits, does not give up and does not fail
        time.sleep(BulkInsertOperation._ENQUEUE_TIMEOUT_IN_SECONDS * 6)
        self.assertEqual([], failures, "waiting for a free slot must not fail the bulk insert")
        self.assertTrue(producer.is_alive(), "the caller stopped waiting while the queue was still full")

        while producer.is_alive():
            try:
                queue.get(timeout=0.05)
            except Exception:
                pass
        producer.join(30)

        self.assertEqual([], failures)
        self.assertEqual(self.DOCUMENT_COUNT, len(stored))

    def test_a_request_that_already_finished_does_not_leave_the_caller_waiting(self):
        bulk = self._bulk_insert_with_a_stalled_stream()
        queue = bulk._buffer_exposer._buffers_to_flush_queue
        finished = Future()
        finished.set_result(None)
        bulk._ongoing_bulk_insert_execute_task = finished
        for _ in range(queue.maxsize):
            queue.put(bytearray(b"waiting to be sent"))

        def store_documents():
            bulk.store(Document("documents/1", self.PAYLOAD * 5))
            bulk.store(Document("documents/2", "small"))

        producer = threading.Thread(target=store_documents, daemon=True)
        producer.start()
        producer.join(15)

        self.assertFalse(producer.is_alive(), "the caller waited for a request that had already finished")
        # the failure is reported through the operation, the way every write failure in a bulk insert is
        self.assertIsInstance(bulk._buffer_exposer._ongoing_operation.exception(timeout=0), BulkInsertAbortedException)


if __name__ == "__main__":
    unittest.main()
