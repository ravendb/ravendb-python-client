"""
Integration tests against a live RavenDB 7.2.3 server for the query tag feature.

Verifies that .with_tag(...) results in a request URL the server accepts and
returns a successful result for. The server doesn't echo the tag back in any
response field, so we can only assert "the tag did not break the request".
"""

import unittest
from typing import Optional

from ravendb.tests.test_base import TestBase


class _User:
    def __init__(self, name: Optional[str] = None, age: Optional[int] = None):
        self.name = name
        self.age = age


class TestQueryTagIntegration(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as s:
            s.store(_User("alice", 30), "users/1")
            s.store(_User("bob", 25), "users/2")
            s.save_changes()

    def test_with_tag_query_succeeds(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_User).with_tag("integration-test").where_greater_than("age", 20))
            self.assertEqual(2, len(results))

    def test_with_tag_raw_query_succeeds(self):
        with self.store.open_session() as s:
            results = list(
                s.advanced.raw_query("from '_Users' where age > 20", object_type=_User).with_tag("integration-test-raw")
            )
            self.assertEqual(2, len(results))

    def test_with_tag_rejects_empty(self):
        with self.store.open_session() as s:
            with self.assertRaises(ValueError):
                s.query(object_type=_User).with_tag("")

    def test_with_tag_lazy_query_succeeds(self):
        # Exercises LazyQueryOperation which appends &tag= to the multi_get
        # GetRequest query string.
        with self.store.open_session() as s:
            lazy = s.query(object_type=_User).with_tag("integration-lazy").where_greater_than("age", 20).lazily()
            results = lazy.value
            self.assertEqual(2, len(results))

    def test_with_tag_stream_query_succeeds(self):
        # Exercises QueryStreamCommand which appends &tag= to the
        # /streams/queries URL.
        with self.store.open_session() as s:
            query = s.query(object_type=_User).with_tag("integration-stream").where_greater_than("age", 20)
            count = 0
            for item in s.advanced.stream(query):
                count += 1
                self.assertIsNotNone(item)
            self.assertEqual(2, count)


if __name__ == "__main__":
    unittest.main()
