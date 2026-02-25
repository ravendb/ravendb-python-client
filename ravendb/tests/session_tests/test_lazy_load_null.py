"""
Lazy load: lazily.load(None) returns a Lazy that resolves to None.

C# reference: SlowTests/Issues/RavenDB_21859.cs
  Load_And_Lazy_Load_Should_Return_Null_When_Id_Is_Null
"""

from ravendb.tests.test_base import TestBase


class User:
    def __init__(self, name: str = None):
        self.name = name


class TestRavenDB21859(TestBase):
    def setUp(self):
        super().setUp()

    def test_load_null_id_returns_none(self):
        with self.store.open_session() as session:
            result = session.load(None, User)
            self.assertIsNone(result)

    def test_lazily_load_null_id_returns_none(self):
        with self.store.open_session() as session:
            lazy = session.advanced.lazily.load(None, User)
            session.advanced.eagerly.execute_all_pending_lazy_operations()
            self.assertIsNone(lazy.value)
