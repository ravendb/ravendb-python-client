import json
import threading
import time
import unittest
from concurrent.futures import Future, ThreadPoolExecutor

from ravendb.documents.bulk_insert_operation import BulkInsertOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.tests.test_base import TestBase


class Document:
    def __init__(self, Id: str = None, payload: str = None):
        self.Id = Id
        self.payload = payload


class _FakeRequestExecutor:
    def __init__(self, conventions: DocumentConventions):
        self.conventions = conventions


class _FakeStore:
    """Enough of a document store to drive the bulk insert buffering, with no server behind it."""

    def __init__(self):
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=2)
        self.conventions = DocumentConventions()

    def get_request_executor(self, database: str) -> _FakeRequestExecutor:
        return _FakeRequestExecutor(self.conventions)


class TestBulkInsertBufferHandover(unittest.TestCase):
    """A finished buffer is handed to the sender uncopied, which holds only while nobody writes into it after."""

    DOCUMENT_COUNT = 120
    PAYLOAD = "p" * 40 * 1024

    def test_a_buffer_never_changes_after_it_has_been_handed_over(self):
        bulk = BulkInsertOperation("db", _FakeStore(), None)
        bulk._operation_id = 1
        bulk._ongoing_bulk_insert_execute_task = Future()
        bulk._current_data_buffer += bytearray("[", encoding="utf-8")

        queue = bulk._buffer_exposer._buffers_to_flush_queue
        handed_over = []  # (the object the sender holds, a snapshot taken the moment it arrived)
        stop = threading.Event()

        def sender():
            while not stop.is_set():
                try:
                    chunk = queue.get(timeout=0.02)
                except Exception:
                    continue
                handed_over.append((chunk, bytes(chunk)))

        reader = threading.Thread(target=sender, daemon=True)
        reader.start()

        buffer_identities = set()
        for i in range(self.DOCUMENT_COUNT):
            bulk.store(Document(f"documents/{i}", f"{i}:{self.PAYLOAD}"))
            buffer_identities.add(id(bulk._current_data_buffer))

        deadline = time.time() + 30
        while queue.qsize() and time.time() < deadline:
            time.sleep(0.01)
        stop.set()
        reader.join(30)
        tail = bytes(bulk._current_data_buffer)

        self.assertGreaterEqual(len(handed_over), 3, "no mid-stream flush happened, the test proves nothing")
        for index, (chunk, snapshot) in enumerate(handed_over):
            self.assertEqual(snapshot, bytes(chunk), f"buffer {index} changed after it was handed over")
        self.assertEqual(
            len(handed_over),
            len({id(chunk) for chunk, _ in handed_over}),
            "the same buffer object was handed over more than once",
        )
        self.assertGreater(len(buffer_identities), 1, "the caller kept writing into the same buffer object")

        commands = json.loads(b"".join(snapshot for _, snapshot in handed_over) + tail + b"]")
        self.assertEqual(self.DOCUMENT_COUNT, len(commands))
        for i, command in enumerate(commands):
            self.assertEqual(f"documents/{i}", command["Id"])
            self.assertEqual(f"{i}:{self.PAYLOAD}", command["Document"]["payload"])


class TestBulkInsertBufferHandoverAgainstServer(TestBase):
    """The same handover, but through the requests library and a real server."""

    DOCUMENT_COUNT = 200
    PAYLOAD = "q" * 26 * 1024  # ~5 MiB in total, so the 1 MiB buffer is flushed several times

    def test_a_load_spanning_many_buffers_arrives_intact(self):
        with self.store.bulk_insert() as bulk:
            for i in range(self.DOCUMENT_COUNT):
                bulk.store(Document(f"documents/{i}", f"{i}:{self.PAYLOAD}"))

        with self.store.open_session() as session:
            self.assertEqual(self.DOCUMENT_COUNT, session.query(object_type=Document).count())

        for i in (0, self.DOCUMENT_COUNT - 1):
            with self.store.open_session() as session:
                loaded = session.load(f"documents/{i}", Document)
                self.assertEqual(f"{i}:{self.PAYLOAD}", loaded.payload, f"documents/{i} arrived corrupted")


if __name__ == "__main__":
    unittest.main()
