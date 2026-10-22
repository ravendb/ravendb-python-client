import unittest
from typing import Optional

from ravendb import OptimisticConcurrencyMode, SessionOptions, TransactionMode
from ravendb.exceptions.raven_exceptions import ConcurrencyException
from ravendb.tests.test_base import TestBase

_STALE_CHANGE_VECTOR = "A:1-aaaaaaaaaaaaaaaaaaaaaa"


class _SimpleDoc:
    def __init__(self, name: Optional[str] = None):
        self.name = name


class TestRegisterForConcurrencyCheck(TestBase):
    def _seed(self, watched: bool = True, other: bool = True) -> None:
        with self.store.open_session() as session:
            if watched:
                session.store(_SimpleDoc("Original"), "docs/watched")
            if other:
                session.store(_SimpleDoc("Other"), "docs/other")
            session.save_changes()

    def _change_vector_of_watched(self) -> str:
        with self.store.open_session() as session:
            return session.advanced.get_change_vector_for(session.load("docs/watched", _SimpleDoc))

    def _modify_watched_in_background(self) -> None:
        with self.store.open_session() as background:
            background.load("docs/watched", _SimpleDoc).name = "Changed"
            background.save_changes()

    def _load_name(self, key: str) -> str:
        with self.store.open_session() as session:
            return session.load(key, _SimpleDoc).name

    def test_with_empty_id_should_throw(self):
        with self.store.open_session() as session:
            with self.assertRaises(ValueError):
                session.advanced.register_for_concurrency_check(None, "cv")
            with self.assertRaises(ValueError):
                session.advanced.register_for_concurrency_check("", "cv")

    def test_throws_when_registered_document_was_modified(self):
        self._seed()
        change_vector = self._change_vector_of_watched()
        self._modify_watched_in_background()

        # the default NONE mode proves the registered check is honored regardless of the mode
        with self.store.open_session() as session:
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.advanced.register_for_concurrency_check("docs/watched", change_vector)
            with self.assertRaises(ConcurrencyException):
                session.save_changes()

        # the whole batch is rolled back
        self.assertEqual("Other", self._load_name("docs/other"))

    def test_succeeds_when_registered_document_unchanged(self):
        self._seed()
        change_vector = self._change_vector_of_watched()

        with self.store.open_session() as session:
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.advanced.register_for_concurrency_check("docs/watched", change_vector)
            session.save_changes()

        self.assertEqual("Edited", self._load_name("docs/other"))

    def test_empty_change_vector_asserts_document_does_not_exist(self):
        self._seed(watched=False)
        with self.store.open_session() as background:
            background.store(_SimpleDoc("Now exists"), "docs/watched")
            background.save_changes()

        with self.store.open_session() as session:
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.advanced.register_for_concurrency_check("docs/watched", "")
            with self.assertRaises(ConcurrencyException):
                session.save_changes()

    def test_empty_change_vector_succeeds_when_document_still_absent(self):
        self._seed(watched=False)

        with self.store.open_session() as session:
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.advanced.register_for_concurrency_check("docs/missing", "")
            session.save_changes()

    def test_none_change_vector_disables_check_for_id(self):
        self._seed()
        options = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)
        with self.store.open_session(session_options=options) as session:
            # under WRITES_AND_READS this load is tracked and would trigger a concurrency check
            session.load("docs/watched", _SimpleDoc)
            session.load("docs/other", _SimpleDoc).name = "Edited"
            self._modify_watched_in_background()

            session.advanced.register_for_concurrency_check("docs/watched", None)
            session.save_changes()

    def test_is_consumed_by_a_successful_save_changes_after_modify(self):
        self._seed()
        with self.store.open_session() as session:
            watched = session.load("docs/watched", _SimpleDoc)
            session.advanced.register_for_concurrency_check(
                "docs/watched", session.advanced.get_change_vector_for(watched)
            )
            watched.name = "Changed"
            session.save_changes()

            # must not re-assert the change vector this session already replaced
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.save_changes()

        self.assertEqual("Changed", self._load_name("docs/watched"))
        self.assertEqual("Edited", self._load_name("docs/other"))

    def test_is_consumed_by_a_successful_save_changes_after_delete(self):
        self._seed()
        with self.store.open_session() as session:
            watched = session.load("docs/watched", _SimpleDoc)
            session.advanced.register_for_concurrency_check(
                "docs/watched", session.advanced.get_change_vector_for(watched)
            )
            session.delete(watched)
            session.save_changes()

            # must not assert the old change vector against this session's own tombstone
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.save_changes()

    def test_is_kept_when_save_changes_fails(self):
        self._seed()
        stale_change_vector = self._change_vector_of_watched()
        self._modify_watched_in_background()

        with self.store.open_session() as session:
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.advanced.register_for_concurrency_check("docs/watched", stale_change_vector)
            with self.assertRaises(ConcurrencyException):
                session.save_changes()
            # a failed save keeps the registration, so a retry fails the same way
            with self.assertRaises(ConcurrencyException):
                session.save_changes()

    def test_explicit_change_vector_is_not_overwritten_by_a_later_load(self):
        self._seed()
        stale_change_vector = self._change_vector_of_watched()
        self._modify_watched_in_background()

        options = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)
        with self.store.open_session(session_options=options) as session:
            session.advanced.register_for_concurrency_check("docs/watched", stale_change_vector)
            session.load("docs/watched", _SimpleDoc)
            session.load("docs/other", _SimpleDoc).name = "Edited"
            with self.assertRaises(ConcurrencyException):
                session.save_changes()

    def test_disabled_check_survives_a_later_load(self):
        self._seed()
        options = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)
        with self.store.open_session(session_options=options) as session:
            session.advanced.register_for_concurrency_check("docs/watched", None)
            session.load("docs/watched", _SimpleDoc)
            self._modify_watched_in_background()
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.save_changes()

    def test_is_honored_when_document_is_also_written_with_its_own_concurrency_check(self):
        self._seed(other=False)
        stale_change_vector = self._change_vector_of_watched()
        self._modify_watched_in_background()

        options = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)
        with self.store.open_session(session_options=options) as session:
            watched = session.load("docs/watched", _SimpleDoc)
            session.advanced.register_for_concurrency_check("docs/watched", stale_change_vector)
            watched.name = "Edited"
            # the write carries the current change vector and would pass, so only the registered check can fail
            with self.assertRaises(ConcurrencyException):
                session.save_changes()

    def test_last_registration_for_an_id_wins(self):
        self._seed()
        with self.store.open_session() as session:
            session.advanced.register_for_concurrency_check("docs/watched", _STALE_CHANGE_VECTOR)
            session.advanced.register_for_concurrency_check("docs/watched", None)
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.save_changes()

        self.assertEqual("Edited", self._load_name("docs/other"))

    def test_is_cleared_by_advanced_clear(self):
        self._seed()
        stale_change_vector = self._change_vector_of_watched()
        self._modify_watched_in_background()

        with self.store.open_session() as session:
            session.advanced.register_for_concurrency_check("docs/watched", stale_change_vector)
            session.advanced.clear()
            session.load("docs/other", _SimpleDoc).name = "Edited"
            session.save_changes()

    def test_on_its_own_sends_exactly_one_request(self):
        self._seed(other=False)
        change_vector = self._change_vector_of_watched()

        with self.store.open_session() as session:
            session.advanced.register_for_concurrency_check("docs/watched", change_vector)
            session.save_changes()
            self.assertEqual(1, session.advanced.number_of_requests)

    def test_disabled_only_sends_no_request(self):
        with self.store.open_session() as session:
            session.advanced.register_for_concurrency_check("docs/watched", _STALE_CHANGE_VECTOR)
            session.advanced.register_for_concurrency_check("docs/watched", None)
            session.save_changes()
            self.assertEqual(0, session.advanced.number_of_requests)

    def test_is_not_supported_in_a_no_tracking_session(self):
        with self.store.open_session(session_options=SessionOptions(no_tracking=True)) as session:
            session.advanced.register_for_concurrency_check("docs/watched", _STALE_CHANGE_VECTOR)
            with self.assertRaises(RuntimeError):
                session.save_changes()

    def test_is_not_supported_in_a_cluster_wide_session(self):
        options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        with self.store.open_session(session_options=options) as session:
            session.advanced.register_for_concurrency_check("docs/watched", _STALE_CHANGE_VECTOR)
            with self.assertRaises(ValueError) as cm:
                session.save_changes()
            self.assertIn("register_for_concurrency_check", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
