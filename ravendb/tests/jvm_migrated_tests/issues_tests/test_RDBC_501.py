from datetime import timedelta
from typing import Dict, Tuple, Optional

from ravendb.documents.queries.time_series import TimeSeriesAggregationResult
from ravendb.documents.session.time_series import ITimeSeriesValuesBindable
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class MarkerSymbol:
    def __init__(self, Id: str = None):
        self.Id = Id


class SymbolPrice(ITimeSeriesValuesBindable):
    def __init__(self, open: float = None, close: float = None, high: float = None, low: float = None):
        self.open = open
        self.close = close
        self.high = high
        self.low = low

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {
            0: ("open", None),
            1: ("close", None),
            2: ("high", None),
            3: ("low", None),
        }


class TestRDBC501(TestBase):
    def setUp(self):
        super(TestRDBC501, self).setUp()

    def test_should_properly_map_typed_entries(self):
        base_line = RavenTestHelper.utc_today()
        with self.store.open_session() as session:
            symbol = MarkerSymbol()
            session.store(symbol, "markerSymbols/1-A")

            price1 = SymbolPrice(4, 7, 10, 1)
            price2 = SymbolPrice(24, 27, 210, 21)
            price3 = SymbolPrice(34, 37, 310, 321)

            tsf = session.typed_time_series_for_entity(SymbolPrice, symbol, "history")
            tsf.append(base_line + timedelta(hours=1), price1)
            tsf.append(base_line + timedelta(hours=2), price2)
            tsf.append(base_line + timedelta(days=2), price3)

            session.save_changes()

        with self.store.open_session() as session:
            aggregated_history_query_result = (
                session.query(object_type=MarkerSymbol)
                .select_time_series(
                    TimeSeriesAggregationResult,
                    lambda b: b.raw(
                        "from history\n"
                        + "          group by '1 days'\n"
                        + "          select first(), last(), min(), max()"
                    ),
                )
                .first()
            )

            self.assertEqual(2, len(aggregated_history_query_result.results))

            typed = aggregated_history_query_result.as_typed_result(SymbolPrice)

            self.assertEqual(2, len(typed.results))

            first_result = typed.results[0]
            self.assertEqual(4, first_result.min.open)
            self.assertEqual(7, first_result.min.close)
            self.assertEqual(1, first_result.min.low)
            self.assertEqual(10, first_result.min.high)

            self.assertEqual(4, first_result.first.open)
            self.assertEqual(7, first_result.first.close)
            self.assertEqual(1, first_result.first.low)
            self.assertEqual(10, first_result.first.high)

            second_result = typed.results[1]
            self.assertEqual(34, second_result.min.open)
            self.assertEqual(37, second_result.min.close)
            self.assertEqual(321, second_result.min.low)
            self.assertEqual(310, second_result.min.high)
