from ravendb.documents.queries.time_series import TimeSeriesRawResult
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestRavenDB15792(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_query_time_series_with_spaces_in_name(self):
        document_id = "users/ayende"
        base_line = RavenTestHelper.utc_this_month()

        with self.store.open_session() as session:
            session.store(User(), document_id)

            tsf = session.time_series_for(document_id, "gas m3 usage")
            tsf.append_single(base_line, 1)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.advanced.raw_query(
                "declare timeseries out()\n" "{\n" '    from "gas m3 usage"\n' "}\n" "from Users as u\n" "select out()",
                TimeSeriesRawResult,
            )

            result = query.first()
            self.assertIsNotNone(result)

            results = result.results

            self.assertEqual(1, len(results))
