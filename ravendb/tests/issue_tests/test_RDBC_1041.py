"""
RDBC-1041: DocumentQuery date-component filter methods.

Adds where_year, where_month, where_day_of_month, where_hour, where_minute,
where_second, and where_ticks, each with equality and comparison-operator variants
(greater_than, greater_than_or_equal, less_than, less_than_or_equal, between).

C# reference: QueryDateTime.cs (FastTests.Client).  The C# client uses LINQ
expression trees translated to property-access form "Date.Year = $p0".  Python
constructs the same dot-notation path directly ("date.Year = $p0").

where_ticks accepts a raw .NET tick count (100-nanosecond intervals since 0001-01-01),
matching the C# DateTime.Ticks property.

ANCIENT DATES (year < ~1000):
  The server cannot reliably extract date components from ISO 8601 strings
  with years below ~1000.  This affects both the dynamic RQL field-path
  syntax used by these methods (date.Day < 7) and static C# LINQ index
  maps (e.date.Day).  The C# client avoids this because its LINQ provider
  compiles to a JavaScript predicate (Date.parse / getDate()) that is
  evaluated at query time; Python has no equivalent mechanism.
"""

import datetime
import unittest

from ravendb.tests.test_base import TestBase


class Event:
    def __init__(self, name: str = None, date: datetime.datetime = None):
        self.name = name
        self.date = date


class TestQueryDatetimeComponentsMethods(unittest.TestCase):
    """Unit tests: method existence and RQL generation (no server required)."""

    # --- method existence ---

    def test_where_year_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_year"))

    def test_where_month_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_month"))

    def test_where_day_of_month_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_day_of_month"))

    def test_where_hour_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_hour"))

    def test_where_minute_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_minute"))

    def test_where_second_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_second"))

    def test_where_ticks_method_exists(self):
        from ravendb.documents.session.query import DocumentQuery

        self.assertTrue(hasattr(DocumentQuery, "where_ticks"))

    def test_comparison_variants_exist(self):
        from ravendb.documents.session.query import DocumentQuery

        suffixes = ["greater_than", "greater_than_or_equal", "less_than", "less_than_or_equal", "between"]
        components = ["year", "month", "day_of_month", "hour", "minute", "second", "ticks"]
        for component in components:
            for suffix in suffixes:
                method = f"where_{component}_{suffix}"
                self.assertTrue(hasattr(DocumentQuery, method), f"Missing method: {method}")

    # --- alias expansion ---

    def test_add_alias_plain_field(self):
        from ravendb.documents.session.tokens.query_tokens.definitions import WhereToken
        from ravendb.documents.session.tokens.misc import WhereOperator

        token = WhereToken.create(WhereOperator.EQUALS, "date", "p0")
        result = token.add_alias("e")
        self.assertEqual("e.date", result.field_name)

    def test_add_alias_dot_notation_field(self):
        from ravendb.documents.session.tokens.query_tokens.definitions import WhereToken
        from ravendb.documents.session.tokens.misc import WhereOperator

        for component in ("Year", "Month", "Day", "Hour", "Minute", "Second", "Ticks"):
            token = WhereToken.create(WhereOperator.EQUALS, f"date.{component}", "p0")
            result = token.add_alias("e")
            self.assertEqual(f"e.date.{component}", result.field_name, f"Failed for {component}")

    def test_add_alias_id_unchanged(self):
        from ravendb.documents.session.tokens.query_tokens.definitions import WhereToken
        from ravendb.documents.session.tokens.misc import WhereOperator

        token = WhereToken.create(WhereOperator.EQUALS, "id()", "p0")
        result = token.add_alias("e")
        self.assertIs(token, result)


class TestQueryDatetimeComponents(TestBase):
    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def _seed(self):
        with self.store.open_session() as session:
            # 7 documents mirroring the C# QueryDateTime.cs test data.
            # C# uses LINQ->JavaScript which handles pre-1000 dates; RQL component
            # extraction (date.Day etc.) requires year >= ~1200, so ancient dates
            # are replaced with medieval equivalents sharing the same component values.
            session.store(Event("Oren", datetime.datetime(1234, 5, 6, 7, 8, 9)), "events/1")
            session.store(Event("Tal", datetime.datetime(1400, 11, 6, 3, 23, 43)), "events/2")
            session.store(Event("Maxim", datetime.datetime(1654, 7, 17, 11, 24, 51)), "events/3")
            session.store(Event("Michael", datetime.datetime(1250, 12, 4, 7, 11, 45)), "events/4")  # was 666
            session.store(Event("Iftah", datetime.datetime(1260, 1, 1, 1, 1, 1)), "events/5")  # was 1
            session.store(Event("Karmel", datetime.datetime(2025, 4, 28, 23, 28, 23)), "events/6")
            session.store(Event("Grisha", datetime.datetime(1300, 2, 17, 19, 23, 31)), "events/7")  # was 11
            session.save_changes()

    # --- RQL generation tests ---

    def test_where_year_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_year("date", 2024).index_query.query
            self.assertIn("date.Year", rql)

    def test_where_year_greater_than_or_equal_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_year_greater_than_or_equal("date", 1400).index_query.query
            self.assertIn("date.Year", rql)
            self.assertIn(">=", rql)

    def test_where_day_of_month_less_than_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_day_of_month_less_than("date", 7).index_query.query
            self.assertIn("date.Day", rql)
            self.assertIn("<", rql)

    def test_where_month_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_month("date", 7).index_query.query
            self.assertIn("date.Month", rql)

    def test_where_hour_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_hour("date", 12).index_query.query
            self.assertIn("date.Hour", rql)

    def test_where_minute_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_minute("date", 30).index_query.query
            self.assertIn("date.Minute", rql)

    def test_where_second_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_second("date", 43).index_query.query
            self.assertIn("date.Second", rql)

    def test_where_ticks_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_ticks("date", 600000000000000000).index_query.query
            self.assertIn("date.Ticks", rql)

    def test_where_ticks_greater_than_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = (
                session.query(object_type=Event).where_ticks_greater_than("date", 600000000000000000).index_query.query
            )
            self.assertIn("date.Ticks", rql)
            self.assertIn(">", rql)

    def test_where_year_between_generates_correct_rql(self):
        with self.store.open_session() as session:
            rql = session.query(object_type=Event).where_year_between("date", 2020, 2025).index_query.query
            self.assertIn("date.Year", rql)
            self.assertIn("between", rql)

    # --- Integration filter tests (mirrors C# QueryDateTime.cs assertions) ---

    def test_year_greater_than_or_equal_1400_returns_3(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_year_greater_than_or_equal("date", 1400))
        self.assertEqual(3, len(results))

    def test_day_less_than_7_returns_4(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_day_of_month_less_than("date", 7))
        self.assertEqual(4, len(results))

    def test_month_range_gt7_lte11_returns_1(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(
                session.query(object_type=Event)
                .where_month_greater_than("date", 7)
                .and_also()
                .where_month_less_than_or_equal("date", 11)
            )
        self.assertEqual(1, len(results))

    def test_hour_greater_than_or_equal_20_returns_1(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_hour_greater_than_or_equal("date", 20))
        self.assertEqual(1, len(results))

    def test_minute_less_than_or_equal_20_returns_3(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_minute_less_than_or_equal("date", 20))
        self.assertEqual(3, len(results))

    def test_second_equals_43_returns_1(self):
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_second("date", 43))
        self.assertEqual(1, len(results))
        self.assertEqual("Tal", results[0].name)

    def test_ticks_greater_than_600000000000000000_returns_1(self):
        # 600000000000000000 .NET ticks = 1902-04-30T10:40:00.000Z (mirrors C# QueryDateTime.cs)
        # Only Karmel (2025-04-28) has ticks above this threshold in the seeded data.
        self._seed()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_ticks_greater_than("date", 600000000000000000))
        self.assertEqual(1, len(results))
        self.assertEqual("Karmel", results[0].name)

    def test_where_year_filters_documents(self):
        with self.store.open_session() as session:
            session.store(Event("New Year", datetime.datetime(2024, 1, 1, 0, 0, 0)), "events/a1")
            session.store(Event("Summer", datetime.datetime(2024, 7, 15, 12, 30, 45)), "events/a2")
            session.store(Event("Xmas", datetime.datetime(2024, 12, 25, 18, 0, 0)), "events/a3")
            session.save_changes()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_year("date", 2024))
        self.assertEqual(3, len(results))

    def test_where_month_filters_documents(self):
        with self.store.open_session() as session:
            session.store(Event("New Year", datetime.datetime(2024, 1, 1, 0, 0, 0)), "events/b1")
            session.store(Event("Summer", datetime.datetime(2024, 7, 15, 12, 30, 45)), "events/b2")
            session.store(Event("Xmas", datetime.datetime(2024, 12, 25, 18, 0, 0)), "events/b3")
            session.save_changes()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_month("date", 7))
        self.assertEqual(1, len(results))
        self.assertEqual("Summer", results[0].name)

    def test_where_day_of_month_filters_documents(self):
        with self.store.open_session() as session:
            session.store(Event("New Year", datetime.datetime(2024, 1, 1, 0, 0, 0)), "events/c1")
            session.store(Event("Summer", datetime.datetime(2024, 7, 15, 12, 30, 45)), "events/c2")
            session.store(Event("Xmas", datetime.datetime(2024, 12, 25, 18, 0, 0)), "events/c3")
            session.save_changes()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_day_of_month("date", 25))
        self.assertEqual(1, len(results))
        self.assertEqual("Xmas", results[0].name)

    def test_where_second_filters_documents(self):
        with self.store.open_session() as session:
            session.store(Event("New Year", datetime.datetime(2024, 1, 1, 0, 0, 0)), "events/d1")
            session.store(Event("Summer", datetime.datetime(2024, 7, 15, 12, 30, 45)), "events/d2")
            session.store(Event("Xmas", datetime.datetime(2024, 12, 25, 18, 0, 0)), "events/d3")
            session.save_changes()
        with self.store.open_session() as session:
            results = list(session.query(object_type=Event).where_second("date", 45))
        self.assertEqual(1, len(results))
        self.assertEqual("Summer", results[0].name)


if __name__ == "__main__":
    unittest.main()
