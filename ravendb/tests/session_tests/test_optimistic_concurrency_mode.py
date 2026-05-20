"""
Unit tests for OptimisticConcurrencyMode (None / Writes / WritesAndReads),
the SessionOptions and DocumentConventions plumbing, and the back-compat
session.advanced.use_optimistic_concurrency shim.

Pure client-side tests — no embedded server needed. Integration coverage
against a 7.2.3 server lives in test_optimistic_concurrency_mode_integration.py.
"""

import unittest

from ravendb import OptimisticConcurrencyMode, SessionOptions, TransactionMode
from ravendb.documents.conventions import DocumentConventions


class TestOptimisticConcurrencyModeOnConventions(unittest.TestCase):
    def test_default_is_none(self):
        c = DocumentConventions()
        self.assertEqual(OptimisticConcurrencyMode.NONE, c.optimistic_concurrency_mode)
        self.assertFalse(c.use_optimistic_concurrency)

    def test_legacy_bool_true_maps_to_writes(self):
        c = DocumentConventions()
        c.use_optimistic_concurrency = True
        self.assertEqual(OptimisticConcurrencyMode.WRITES, c.optimistic_concurrency_mode)
        self.assertTrue(c.use_optimistic_concurrency)

    def test_legacy_bool_false_maps_to_none(self):
        c = DocumentConventions()
        c.use_optimistic_concurrency = False
        self.assertEqual(OptimisticConcurrencyMode.NONE, c.optimistic_concurrency_mode)
        self.assertFalse(c.use_optimistic_concurrency)

    def test_mode_set_after_bool_raises(self):
        c = DocumentConventions()
        c.use_optimistic_concurrency = True
        with self.assertRaises(RuntimeError) as cm:
            c.optimistic_concurrency_mode = OptimisticConcurrencyMode.WRITES_AND_READS
        self.assertIn("optimistic_concurrency_mode", str(cm.exception))
        self.assertIn("use_optimistic_concurrency", str(cm.exception))

    def test_bool_set_after_mode_raises(self):
        c = DocumentConventions()
        c.optimistic_concurrency_mode = OptimisticConcurrencyMode.WRITES_AND_READS
        with self.assertRaises(RuntimeError) as cm:
            c.use_optimistic_concurrency = True
        self.assertIn("use_optimistic_concurrency", str(cm.exception))

    def test_clone_carries_mode(self):
        c = DocumentConventions()
        c.optimistic_concurrency_mode = OptimisticConcurrencyMode.WRITES_AND_READS
        clone = c.clone()
        self.assertEqual(OptimisticConcurrencyMode.WRITES_AND_READS, clone.optimistic_concurrency_mode)


class TestSessionOptionsValidation(unittest.TestCase):
    def test_no_tracking_plus_writes_rejected(self):
        with self.assertRaises(RuntimeError):
            SessionOptions(no_tracking=True, optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)

    def test_no_tracking_plus_writes_and_reads_rejected(self):
        with self.assertRaises(RuntimeError):
            SessionOptions(no_tracking=True, optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)

    def test_no_tracking_plus_none_allowed(self):
        # Should not raise.
        SessionOptions(no_tracking=True, optimistic_concurrency_mode=OptimisticConcurrencyMode.NONE)

    def test_cluster_wide_plus_writes_rejected(self):
        with self.assertRaises(RuntimeError):
            SessionOptions(
                transaction_mode=TransactionMode.CLUSTER_WIDE,
                optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES,
            )

    def test_mode_setter_validates_after_construction(self):
        opts = SessionOptions(no_tracking=True)
        with self.assertRaises(RuntimeError):
            opts.optimistic_concurrency_mode = OptimisticConcurrencyMode.WRITES

    def test_no_tracking_setter_validates_against_mode(self):
        opts = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES_AND_READS)
        with self.assertRaises(RuntimeError):
            opts.no_tracking = True

    def test_transaction_mode_setter_validates_against_mode(self):
        opts = SessionOptions(optimistic_concurrency_mode=OptimisticConcurrencyMode.WRITES)
        with self.assertRaises(RuntimeError):
            opts.transaction_mode = TransactionMode.CLUSTER_WIDE


class TestBatchTrackChangesCommand(unittest.TestCase):
    def test_serialize_skips_ids_already_checked(self):
        from ravendb.documents.commands.batches import BatchTrackChangesCommandData, CommandType

        cmd = BatchTrackChangesCommandData(
            tracked_entities={"docs/1": "cv-1", "docs/2": "cv-2", "docs/3": "cv-3"},
            ids_to_skip={"docs/2"},
        )
        serialized = cmd.serialize(None)
        self.assertEqual(str(CommandType.BATCH_TRACK_CHANGES), serialized["Type"])
        self.assertEqual({"docs/1": "cv-1", "docs/3": "cv-3"}, serialized["TrackedEntities"])

    def test_command_type_round_trips(self):
        from ravendb.documents.commands.batches import CommandType

        self.assertEqual(CommandType.BATCH_TRACK_CHANGES, CommandType.from_csharp_value_str("BatchTrackChanges"))


if __name__ == "__main__":
    unittest.main()
