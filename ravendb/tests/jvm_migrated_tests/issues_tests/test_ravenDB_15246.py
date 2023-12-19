from datetime import timedelta

from ravendb.documents.operations.time_series import GetMultipleTimeSeriesOperation
from ravendb.documents.session.time_series import TimeSeriesRange
from ravendb.primitives.constants import int_max
from ravendb.tests.test_base import TestBase, User
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestRavenDB15246(TestBase):
    def setUp(self):
        super().setUp()

    def test_results_with_range_and_page_size(self):
        tag = "raven"
        id = "users/1"
        base_line = RavenTestHelper.utc_this_month()

        with self.store.open_session() as session:
            session.store(User(), id)
            tsf = session.time_series_for(id, tag)
            for i in range(16):
                tsf.append_single(base_line + timedelta(minutes=i), i, "watches/apple")

            session.save_changes()

        ranges_list = [
            TimeSeriesRange("raven", base_line + timedelta(minutes=0), base_line + timedelta(minutes=3)),
            TimeSeriesRange("raven", base_line + timedelta(minutes=4), base_line + timedelta(minutes=7)),
            TimeSeriesRange("raven", base_line + timedelta(minutes=8), base_line + timedelta(minutes=11)),
        ]

        re = self.store.get_request_executor()

        ts_command = GetMultipleTimeSeriesOperation.GetMultipleTimeSeriesCommand(id, ranges_list, 0, 0)
        re.execute_command(ts_command)

        res = ts_command.result
        self.assertEqual(0, len(res.values))

        ts_command = GetMultipleTimeSeriesOperation.GetMultipleTimeSeriesCommand(id, ranges_list, 0, 30)
        re.execute_command(ts_command)

        res = ts_command.result
        self.assertEqual(1, len(res.values))
        self.assertEqual(3, len(res.values.get("raven")))

        self.assertEqual(4, len(res.values.get("raven")[0].entries))
        self.assertEqual(4, len(res.values.get("raven")[1].entries))
        self.assertEqual(4, len(res.values.get("raven")[2].entries))

        ts_command = GetMultipleTimeSeriesOperation.GetMultipleTimeSeriesCommand(id, ranges_list, 0, 6)
        re.execute_command(ts_command)

        res = ts_command.result

        self.assertEqual(1, len(res.values))
        self.assertEqual(2, len(res.values.get("raven")))

        self.assertEqual(4, len(res.values.get("raven")[0].entries))
        self.assertEqual(2, len(res.values.get("raven")[1].entries))

    def test_client_cache_with_start(self):
        base_line = RavenTestHelper.utc_this_month()

        with self.store.open_session() as session:
            session.store(User(), "users/1-A")
            tsf = session.time_series_for("users/1-A", "Heartrate")
            for i in range(20):
                tsf.append_single(base_line + timedelta(minutes=i), i, "watches/apple")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            ts = session.time_series_for_entity(user, "Heartrate")

            self.assertEqual(1, session.advanced.number_of_requests)

            res = ts.get(start=20)
            self.assertEqual(0, len(res))
            self.assertEqual(2, session.advanced.number_of_requests)

            res = ts.get(start=10)
            self.assertEqual(10, len(res))
            self.assertEqual(base_line + timedelta(minutes=10), res[0].timestamp)
            self.assertEqual(3, session.advanced.number_of_requests)

            res = ts.get(start=0)
            self.assertEqual(20, len(res))
            self.assertEqual(base_line + timedelta(minutes=10), res[10].timestamp)
            self.assertEqual(4, session.advanced.number_of_requests)

            res = ts.get(start=10)
            self.assertEqual(10, len(res))
            self.assertEqual(base_line + timedelta(minutes=10), res[0].timestamp)
            self.assertEqual(4, session.advanced.number_of_requests)

            res = ts.get(start=20)
            self.assertEqual(0, len(res))
