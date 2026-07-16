"""
Serialization: entities whose dict fields use Enum keys store and round-trip
correctly; Enum keys are serialized as their string values.

C# reference: FastTests/Issues/RavenDB_22084.cs
  StoreOnDictionaryWithEnumKeyShouldWork
"""

from enum import Enum

from ravendb.tests.test_base import TestBase


class CheckType(Enum):
    ENGINE = "Engine"
    GEARS = "Gears"


class CheckStatus(Enum):
    GOOD = "Good"
    BAD = "Bad"


class Machine:
    def __init__(self, checks=None):
        self.checks = checks or {}


class TestRavenDB22084(TestBase):
    def setUp(self):
        super().setUp()

    def test_store_entity_with_enum_keyed_dict(self):
        """
        C# spec: StoreOnDictionaryWithEnumKeyShouldWork
          var m = new Machine { Checks = { [CheckType.Engine] = CheckStatus.Good, ... } }
          session.Store(m)
          session.SaveChanges()  → succeeds; Enum keys are serialized as their string values.
        """
        m = Machine(checks={CheckType.ENGINE: CheckStatus.GOOD, CheckType.GEARS: CheckStatus.GOOD})
        with self.store.open_session() as session:
            session.store(m, "machines/1")
            session.save_changes()

    def test_round_trip_preserves_enum_dict_values(self):
        """
        C# spec: after StoreOnDictionaryWithEnumKeyShouldWork, loading the document back
        gives Checks[CheckType.Engine] == CheckStatus.Good.
        """
        m = Machine(checks={CheckType.ENGINE: CheckStatus.GOOD, CheckType.GEARS: CheckStatus.BAD})
        with self.store.open_session() as session:
            session.store(m, "machines/2")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load("machines/2", Machine)
            self.assertIsNotNone(loaded, "Document should be loadable after store")
            # JSON deserializes back to string keys; verify the enum values were stored correctly.
            self.assertEqual("Good", loaded.checks.get("Engine"), "Engine check should be Good")
            self.assertEqual("Bad", loaded.checks.get("Gears"), "Gears check should be Bad")

    # ------------------------------------------------------------------ #
    #  Baseline: dict with string keys                                    #
    # ------------------------------------------------------------------ #

    def test_store_entity_with_string_keyed_dict_works(self):
        """
        Baseline: when dict keys are plain strings, store() works without error.
        Confirms the serialization layer itself is functional.
        """
        m = Machine(checks={"Engine": "Good", "Gears": "Good"})
        with self.store.open_session() as session:
            session.store(m, "machines/3")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load("machines/3", Machine)
            self.assertIsNotNone(loaded)
            self.assertEqual(2, len(loaded.checks))
