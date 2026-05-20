"""
Integration tests against a live RavenDB 7.2.x server for the 7.2.3
disposed-guard additions:

  * `session.save_changes()` raises when the parent store has been disposed
  * The session's own disposed guard fires too
  * `RAVEN_DISABLE_DISPOSE_CHECKS=true` lets sessions still close cleanly
    against a disposed store (escape hatch)
"""

import os
import unittest

from ravendb.tests.test_base import TestBase, User
import ravendb.documents.session.document_session_operations.in_memory_document_session_operations as _session_mod


class TestDisposedGuardsIntegration(TestBase):
    def test_save_changes_raises_after_session_close(self):
        with self.store.open_session() as session:
            session.store(User("alice"), "users/1")
            session.save_changes()
            session.close()
            with self.assertRaises(RuntimeError) as cm:
                session.store(User("bob"), "users/2")
                session.save_changes()
            self.assertIn("disposed", str(cm.exception).lower())

    def test_save_changes_raises_after_store_dispose(self):
        # Open a session, close the store, then try to use the session.
        store = self.get_document_store("test_db_disposed_guard")
        session = store.open_session()
        session.store(User("alice"), "users/1")
        session.save_changes()

        store.close()  # disposes the store

        with self.assertRaises(RuntimeError) as cm:
            session.save_changes()
        self.assertIn("disposed", str(cm.exception).lower())

    def test_dispose_check_env_var_disables_store_guard(self):
        # Same scenario as above but with RAVEN_DISABLE_DISPOSE_CHECKS=true,
        # the store-disposed half of the guard should be skipped.
        # We patch the module sentinel directly (the env var is read at
        # import time, and we already validate that path in the unit tests).
        prev = _session_mod._DISABLE_DISPOSE_CHECKS
        _session_mod._DISABLE_DISPOSE_CHECKS = True
        try:
            store = self.get_document_store("test_db_disposed_envvar")
            session = store.open_session()
            session.store(User("alice"), "users/1")
            session.save_changes()
            store.close()

            # The session-level disposed flag still fires when the session
            # itself is closed. We only verify that calling assert_not_disposed
            # against a closed store no longer raises.
            session.assert_not_disposed()
        finally:
            _session_mod._DISABLE_DISPOSE_CHECKS = prev


if __name__ == "__main__":
    unittest.main()
