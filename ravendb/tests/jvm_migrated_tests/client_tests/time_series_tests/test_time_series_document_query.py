from datetime import timedelta

from ravendb.documents.queries.time_series import TimeSeriesAggregationResult, TimeSeriesRawResult
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestTimeSeriesDocumentQuery(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_query_time_series_using_document_query(self):
        base_line = RavenTestHelper.utc_today()

        with self.store.open_session() as session:
            user = User(name="Oren", age=35)
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=61), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=62), 79, "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=63), 69, "watches/fitbit")

            tsf.append(base_line + timedelta(days=31, minutes=61), [159], "watches/apple")
            tsf.append(base_line + timedelta(days=31, minutes=62), [179], "watches/apple")
            tsf.append(base_line + timedelta(days=31, minutes=63), [169], "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            ts_query_text = (
                "from Heartrate between $start and $end\n"
                "where Tag = 'watches/fitbit'\n"
                "group by '1 month'\n"
                "select min(), max(), avg()"
            )

            query = (
                session.advanced.document_query(object_type=User)
                .where_greater_than("age", 21)
                .select_time_series(TimeSeriesAggregationResult, lambda b: b.raw(ts_query_text))
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=92))
            )

            result = list(query)

            self.assertEqual(1, len(result))
            self.assertEqual(3, result[0].count)

            agg = result[0].results
            self.assertEqual(2, len(agg))

            self.assertEqual(69, agg[0].max[0])
            self.assertEqual(59, agg[0].min[0])
            self.assertEqual(64, agg[0].average[0])

            self.assertEqual(169, agg[1].max[0])
            self.assertEqual(169, agg[1].min[0])
            self.assertEqual(169, agg[1].average[0])

    def test_can_query_time_series_raw_values_using_document_query(self):
        base_line = RavenTestHelper.utc_today()

        with self.store.open_session() as session:
            user = User(name="Oren", age=35)
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=61), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=62), 79, "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=63), 69, "watches/fitbit")

            tsf.append(base_line + timedelta(days=31, minutes=61), [159], "watches/apple")
            tsf.append(base_line + timedelta(days=31, minutes=62), [179], "watches/apple")
            tsf.append(base_line + timedelta(days=31, minutes=63), [169], "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            ts_query_text = "from Heartrate between $start and $end\n" "where Tag = 'watches/fitbit'"

            query = (
                session.advanced.document_query(object_type=User)
                .where_greater_than("age", 21)
                .select_time_series(TimeSeriesRawResult, lambda b: b.raw(ts_query_text))
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=92))
            )

            result = list(query)

            self.assertEqual(1, len(result))
            self.assertEqual(3, result[0].count)

            values = result[0].results

            self.assertEqual(3, len(values))

            self.assertEqual([59], values[0].values)
            self.assertEqual("watches/fitbit", values[0].tag)
            self.assertEqual(base_line + timedelta(minutes=61), values[0].timestamp)

            self.assertEqual([69], values[1].values)
            self.assertEqual("watches/fitbit", values[1].tag)
            self.assertEqual(base_line + timedelta(minutes=63), values[1].timestamp)

            self.assertEqual([169], values[2].values)
            self.assertEqual("watches/fitbit", values[2].tag)
            self.assertEqual(base_line + timedelta(minutes=63, days=31), values[2].timestamp)
