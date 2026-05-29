"""
Unit tests for the 7.2.3 disposed-guard additions:
  * RequestExecutor.execute_command raises after the executor is disposed.
  * RAVEN_DISABLE_DISPOSE_CHECKS env var escapes the cross-component guard.

We avoid importlib.reload here on purpose — reloading
in_memory_document_session_operations swaps out module-level classes
(RefEq, TrackedEntitiesHolder, ...) which then trip identity checks in
unrelated tests sharing the same process. Instead we monkey-patch the
module-level `_DISABLE_DISPOSE_CHECKS` constant directly.
"""

import os
import unittest

import ravendb.documents.session.document_session_operations.in_memory_document_session_operations as _session_mod
from ravendb.http.request_executor import RequestExecutor


class TestDisableDisposeChecksEnvVar(unittest.TestCase):
    def test_sentinel_reads_env_at_import_time(self):
        # The constant is a plain bool computed when the module is first
        # imported. We verify it reflects the current process env without
        # forcing a reload (which would invalidate the class identity of
        # RefEq / TrackedEntitiesHolder for any module that already imported
        # those — and break unrelated tests sharing this process).
        expected = os.environ.get("RAVEN_DISABLE_DISPOSE_CHECKS", "").lower() == "true"
        self.assertEqual(expected, _session_mod._DISABLE_DISPOSE_CHECKS)

    def test_sentinel_is_a_bool(self):
        self.assertIsInstance(_session_mod._DISABLE_DISPOSE_CHECKS, bool)


class _PatchSentinel:
    """Context manager that temporarily flips _DISABLE_DISPOSE_CHECKS."""

    def __init__(self, value: bool):
        self.value = value
        self._prev = None

    def __enter__(self):
        self._prev = _session_mod._DISABLE_DISPOSE_CHECKS
        _session_mod._DISABLE_DISPOSE_CHECKS = self.value
        return self

    def __exit__(self, exc_type, exc, tb):
        _session_mod._DISABLE_DISPOSE_CHECKS = self._prev


class TestRequestExecutorEntryGuard(unittest.TestCase):
    def test_disposed_executor_throws_on_execute_command(self):
        executor = RequestExecutor.__new__(RequestExecutor)
        executor._disposed = True
        with _PatchSentinel(False):
            with self.assertRaises(RuntimeError) as cm:
                executor._throw_if_disposed_at_entry()
        self.assertIn("disposed", str(cm.exception).lower())

    def test_disposed_executor_skipped_when_sentinel_set(self):
        executor = RequestExecutor.__new__(RequestExecutor)
        executor._disposed = True
        with _PatchSentinel(True):
            # Should NOT raise.
            executor._throw_if_disposed_at_entry()

    def test_not_disposed_executor_passes(self):
        executor = RequestExecutor.__new__(RequestExecutor)
        executor._disposed = False
        with _PatchSentinel(False):
            executor._throw_if_disposed_at_entry()
        with _PatchSentinel(True):
            executor._throw_if_disposed_at_entry()


if __name__ == "__main__":
    unittest.main()
