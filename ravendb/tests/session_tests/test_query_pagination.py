"""
Query pagination: skip() without take() returns the expected documents.

C# reference: FastTests/Issues/RavenDB_20542.cs
  AddLongSkipToLINQ
"""

from ravendb.tests.test_base import TestBase


class UserSkip:
    def __init__(self, name: str = ""):
        self.name = name


class TestRavenDB20542(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as session:
            for name in ["AA", "BB", "CC"]:
                session.store(UserSkip(name=name))
            session.save_changes()

    def test_skip_one_without_take_returns_remaining_documents(self):
        """
        C# spec: session.Query<User>().Skip(1).ToList() → 2 results (AA, BB, CC minus 1).
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=UserSkip)
            q = q.skip(1)
            results = list(q)

        self.assertEqual(
            2,
            len(results),
            f"skip(1) on 3 documents should return 2, but got {len(results)}",
        )

    def test_skip_with_max_long_returns_no_results(self):
        """
        C# spec: session.Query<User>().Skip(long.MaxValue).ToList() → 0 results.
        Skipping past all documents returns an empty list.
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=UserSkip)
            q = q.skip(9223372036854775807)
            results = list(q)

        self.assertEqual(
            0,
            len(results),
            f"skip(long.MaxValue) should skip all documents and return 0, got {len(results)}",
        )

    # ------------------------------------------------------------------ #
    #  Baseline: skip() combined with take()                              #
    # ------------------------------------------------------------------ #

    def test_skip_with_take_returns_correct_slice(self):
        """
        Baseline: skip(1).take(10) on 3 documents returns 2.
        """
        with self.store.open_session() as session:
            q = session.advanced.document_query(object_type=UserSkip)
            q = q.skip(1).take(10)
            results = list(q)

        self.assertEqual(
            2,
            len(results),
            f"skip(1).take(10) on 3 documents should return 2, got {len(results)}",
        )
