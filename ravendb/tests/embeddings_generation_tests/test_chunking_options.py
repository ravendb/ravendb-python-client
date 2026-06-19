import unittest

from ravendb.documents.operations.ai.chunking_options import ChunkingOptions, ChunkingMethod


class TestChunkingOptionsContextPrefix(unittest.TestCase):
    def test_context_prefix_serialized(self):
        options = ChunkingOptions(ChunkingMethod.PLAIN_TEXT_SPLIT, 100, 0, context_prefix="Title: Foo")
        self.assertEqual("Title: Foo", options.to_json()["ContextPrefix"])

    def test_context_prefix_round_trip(self):
        options = ChunkingOptions(ChunkingMethod.PLAIN_TEXT_SPLIT, 100, 0, context_prefix="Title: Foo")
        restored = ChunkingOptions.from_json(options.to_json())
        self.assertEqual("Title: Foo", restored.context_prefix)
        self.assertEqual(options, restored)
        self.assertEqual(hash(options), hash(restored))

    def test_equality_is_sensitive_to_context_prefix(self):
        left = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="A")
        right = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="B")
        self.assertNotEqual(left, right)

    def test_validate_rejects_empty_or_whitespace_prefix(self):
        for bad in ("", "   "):
            errors = []
            ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix=bad).validate("source", errors)
            self.assertEqual(1, len(errors), bad)
            self.assertIn("ContextPrefix", errors[0])

    def test_validate_accepts_valid_prefix(self):
        errors = []
        ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="Doc title").validate("source", errors)
        self.assertEqual([], errors)

    def test_no_chunking_marker_is_not_serialized(self):
        options = ChunkingOptions(ChunkingMethod.HTML_STRIP, 100, 0, context_prefix="ok")
        options.no_chunking = True
        self.assertNotIn("NoChunking", options.to_json())

    def test_no_chunking_marker_bypasses_budget_validation(self):
        # max_tokens_per_chunk == 0 would normally be rejected; no_chunking skips those checks.
        options = ChunkingOptions(ChunkingMethod.HTML_STRIP, 0, 0, context_prefix="ok")
        options.no_chunking = True
        errors = []
        options.validate("source", errors)
        self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()
