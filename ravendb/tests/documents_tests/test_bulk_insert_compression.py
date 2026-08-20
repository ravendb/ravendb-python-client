import gzip
import json
import unittest

from ravendb.documents.bulk_insert_operation import BulkInsertOperation, BulkInsertOptions
from ravendb.http.server_node import ServerNode
from ravendb.primitives import constants
from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, Id: str = None, name: str = None, notes: str = None):
        self.Id = Id
        self.name = name
        self.notes = notes


class TestBulkInsertCompressionUnit(unittest.TestCase):
    def test_the_buffers_become_one_smaller_gzip_stream_written_as_it_goes(self):
        buffers = [bytearray(b'[{"Id":"users/1"}'), bytearray(b',{"Id":"users/2"}'), bytearray(b"]")]
        compressed = b"".join(BulkInsertOperation._BulkInsertCommand._gzip(iter(buffers)))
        self.assertEqual(b"".join(bytes(buffer) for buffer in buffers), gzip.decompress(compressed))

        bulky = bytearray(json.dumps([{"Id": f"users/{i}", "name": "the same name"} for i in range(500)]).encode())
        self.assertLess(len(b"".join(BulkInsertOperation._BulkInsertCommand._gzip(iter([bulky])))), len(bulky))

        # a buffer must not sit in the compressor waiting for the next one
        stream = BulkInsertOperation._BulkInsertCommand._gzip(iter([bytearray(b"x" * 4096), bytearray(b"y" * 4096)]))
        self.assertTrue(next(stream))

    def test_the_request_declares_the_encoding_it_used(self):
        def request_with_compression(use_compression: bool):
            command = BulkInsertOperation._BulkInsertCommand(1, BulkInsertOperation._BufferExposer(1), "A", False)
            command.use_compression = use_compression
            return command.create_request(ServerNode("http://localhost:8080", "db"))

        header = constants.Headers.CONTENT_ENCODING
        self.assertEqual(constants.Headers.Encodings.GZIP, request_with_compression(True).headers.get(header))
        self.assertIsNone(request_with_compression(False).headers.get(header))


class TestBulkInsertCompression(TestBase):
    def test_documents_stored_with_compression_arrive_intact(self):
        # a payload that actually compresses, and enough of it to cross a buffer boundary
        notes = "the quick brown fox jumps over the lazy dog " * 2000

        with self.store.bulk_insert(self.store.database, BulkInsertOptions(use_compression=True)) as bulk:
            self.assertTrue(bulk.use_compression)
            for i in range(60):
                bulk.store(User(f"users/{i}", f"user {i}", notes))

        with self.store.open_session() as session:
            self.assertEqual(60, session.query(object_type=User).count())
            loaded = session.load("users/42", User)
            self.assertEqual("user 42", loaded.name)
            self.assertEqual(notes, loaded.notes)


if __name__ == "__main__":
    unittest.main()
