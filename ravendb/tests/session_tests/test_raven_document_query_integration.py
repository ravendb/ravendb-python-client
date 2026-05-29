"""
Integration tests for RavenDocumentQuery.Now / Today / CmpXchg.

Verifies that the RQL emitted by the client (`now()`, `now($offset)`,
`today()`, `cmpxchg($key)`) is accepted and correctly evaluated by a live
7.2.x RavenDB server.

The date tests use offsets large enough (±1 day) that client/server clock
skew on CI does not perturb the assertions.
"""

import datetime
import unittest
from typing import Optional

from ravendb import RavenDocumentQuery
from ravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
)
from ravendb.tests.test_base import TestBase


class _Event:
    def __init__(self, name: Optional[str] = None, at: Optional[datetime.datetime] = None):
        self.name = name
        self.at = at


class _User:
    def __init__(self, name: Optional[str] = None):
        self.name = name


class TestRavenDocumentQueryNowAgainstServer(TestBase):
    def setUp(self):
        super().setUp()
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        with self.store.open_session() as s:
            s.store(_Event("past", now - datetime.timedelta(days=2)), "events/past")
            s.store(_Event("future", now + datetime.timedelta(days=2)), "events/future")
            s.save_changes()

    def test_where_greater_than_now_returns_only_future(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_greater_than("at", RavenDocumentQuery.now()))
            names = sorted(e.name for e in results)
            self.assertEqual(["future"], names)

    def test_where_less_than_now_returns_only_past(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_less_than("at", RavenDocumentQuery.now()))
            names = sorted(e.name for e in results)
            self.assertEqual(["past"], names)

    def test_where_greater_than_now_with_negative_offset_returns_recent_and_future(self):
        # now("-3d") = 3 days ago. The "past" event is 2 days ago, "future" is 2 days ahead.
        # Both are "after 3 days ago".
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_greater_than("at", RavenDocumentQuery.now("-3d")))
            names = sorted(e.name for e in results)
            self.assertEqual(["future", "past"], names)

    def test_where_greater_than_now_with_positive_offset_returns_nothing(self):
        # now("+5d") is 5 days from now. No event is that far in the future.
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_greater_than("at", RavenDocumentQuery.now("+5d")))
            self.assertEqual(0, len(results))


class TestRavenDocumentQueryTodayAgainstServer(TestBase):
    def setUp(self):
        super().setUp()
        today = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        with self.store.open_session() as s:
            s.store(_Event("yesterday", today - datetime.timedelta(hours=12)), "events/yesterday")
            s.store(_Event("today-noon", today + datetime.timedelta(hours=12)), "events/today-noon")
            s.store(_Event("tomorrow", today + datetime.timedelta(days=1, hours=1)), "events/tomorrow")
            s.save_changes()

    def test_where_greater_than_or_equal_today_returns_today_and_future(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_greater_than_or_equal("at", RavenDocumentQuery.today()))
            names = sorted(e.name for e in results)
            self.assertEqual(["today-noon", "tomorrow"], names)

    def test_where_less_than_today_returns_only_yesterday(self):
        with self.store.open_session() as s:
            results = list(s.query(object_type=_Event).where_less_than("at", RavenDocumentQuery.today()))
            names = sorted(e.name for e in results)
            self.assertEqual(["yesterday"], names)


class TestRavenDocumentQueryCmpXchgAgainstServer(TestBase):
    def test_where_equals_cmp_xchg_resolves_at_server_side(self):
        # Server-side cmpxchg lookup: query for the user whose name matches the
        # compare-exchange value `active-user`.
        self.store.operations.send(PutCompareExchangeValueOperation("active-user", "alice", 0))

        with self.store.open_session() as s:
            s.store(_User("alice"), "users/1")
            s.store(_User("bob"), "users/2")
            s.save_changes()

        with self.store.open_session() as s:
            results = list(s.query(object_type=_User).where_equals("name", RavenDocumentQuery.cmp_xchg("active-user")))
            self.assertEqual(1, len(results))
            self.assertEqual("alice", results[0].name)


if __name__ == "__main__":
    unittest.main()
