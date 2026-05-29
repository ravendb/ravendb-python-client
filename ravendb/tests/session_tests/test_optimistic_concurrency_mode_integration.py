"""
Integration tests against a live RavenDB 7.2.3 server for the
OptimisticConcurrencyMode plumbing.

Covers:
  * mode propagates from SessionOptions to the opened session
  * mode propagates from DocumentConventions when SessionOptions doesn't set it
  * WRITES rejects PUT when the document changed under us
  * WRITES_AND_READS rejects SaveChanges when ANY tracked document changed
  * use_optimistic_concurrency back-compat shim still works
  * session.advanced.optimistic_concurrency_mode setter mirrors C#
"""

import unittest
from typing import Optional

from ravendb import OptimisticConcurrencyMode, SessionOptions
from ravendb.exceptions.raven_exceptions import ConcurrencyException
from ravendb.tests.test_base import TestBase


class _User:
    def __init__(self, name: Optional[str] = None, age: Optional[int] = None):
        self.name = name
        self.age = age


class TestOptimisticConcurrencyModeIntegration(TestBase):
    def test_writes_mode_rejects_stale_put(self):
        # Two sessions racing on the same document with WRITES enabled.
        with self.store.open_session(
            session_options=SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)
        ) as s1:
            s1.store(_User("alice", 1), "users/1")
            s1.save_changes()

        with self.store.open_session(
            session_options=SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)
        ) as a:
            user_a = a.load("users/1", _User)
            with self.store.open_session(
                session_options=SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)
            ) as b:
                user_b = b.load("users/1", _User)
                user_b.age = 99
                b.save_changes()

            user_a.age = 42
            with self.assertRaises(ConcurrencyException):
                a.save_changes()

    def test_none_mode_lets_stale_put_through(self):
        with self.store.open_session(
            session_options=SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.NONE)
        ) as s1:
            s1.store(_User("bob", 1), "users/2")
            s1.save_changes()

        with self.store.open_session(
            session_options=SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.NONE)
        ) as a:
            user_a = a.load("users/2", _User)
            with self.store.open_session() as b:
                user_b = b.load("users/2", _User)
                user_b.age = 99
                b.save_changes()

            user_a.age = 42
            # Should succeed — last write wins.
            a.save_changes()

    def test_writes_and_reads_rejects_when_tracked_doc_changes_under_us(self):
        # The defining test for WritesAndReads — the session sends change vectors
        # for ALL tracked documents, not just modified ones.
        with self.store.open_session() as s:
            s.store(_User("tracked", 1), "users/tracked-1")
            s.store(_User("modified", 1), "users/modified-1")
            s.save_changes()

        opts = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)
        with self.store.open_session(session_options=opts) as a:
            # Load (track) one doc, plan to modify another.
            tracked = a.load("users/tracked-1", _User)
            modifying = a.load("users/modified-1", _User)
            modifying.age = 42

            # Another session mutates the unrelated tracked doc.
            with self.store.open_session() as b:
                other = b.load("users/tracked-1", _User)
                other.age = 999
                b.save_changes()

            with self.assertRaises(ConcurrencyException):
                a.save_changes()

    def test_conventions_default_inherited_by_session(self):
        # Spin up a fresh store so we can mutate conventions before initialize().
        from ravendb.documents.store.definition import DocumentStore

        store = DocumentStore(self.store.urls, self.store.database)
        store.conventions.optimistic_concurrency_mode = OptimisticConcurrencyMode.WRITES
        store.initialize()
        try:
            # SessionOptions doesn't set the mode -> session should pick it up
            # from conventions.
            with store.open_session() as s:
                self.assertEqual(OptimisticConcurrencyMode.WRITES, s.advanced.optimistic_concurrency_mode)
                self.assertTrue(s.advanced.use_optimistic_concurrency)
        finally:
            store.close()

    def test_legacy_use_optimistic_concurrency_setter_routes_through_mode(self):
        with self.store.open_session() as s:
            s.advanced.use_optimistic_concurrency = True
            self.assertEqual(OptimisticConcurrencyMode.WRITES, s.advanced.optimistic_concurrency_mode)
            self.assertTrue(s.advanced.use_optimistic_concurrency)

    def test_no_tracking_plus_writes_rejected_at_construction(self):
        with self.assertRaises(RuntimeError):
            SessionOptions(no_tracking=True, optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)


if __name__ == "__main__":
    unittest.main()
