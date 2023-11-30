from datetime import datetime, timedelta

from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesIncludes(TestBase):
    def setUp(self):
        super(TestTimeSeriesIncludes, self).setUp()

    def test_should_cache_empty_time_series_ranges(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, "Heartrate")
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 6, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "Heartrate", base_line - timedelta(minutes=30), base_line - timedelta(minutes=10)
                ),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Gracjan", user.name)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line - timedelta(minutes=30), base_line - timedelta(minutes=10)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            cache = session.time_series_by_doc_id.get(document_id)
            ranges = cache.get("Heartrate")
            self.assertEqual(1, len(ranges))

            self.assertEqual(0, len(ranges[0].entries))

            self.assertEqual(base_line - timedelta(minutes=30), ranges[0].from_date)
            self.assertEqual(base_line - timedelta(minutes=10), ranges[0].to_date)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line - timedelta(minutes=25), base_line - timedelta(minutes=15)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            session.advanced.evict(user)

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "BloodPressure", base_line + timedelta(minutes=10), base_line + timedelta(minutes=30)
                ),
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, "BloodPressure").get(
                base_line + timedelta(minutes=10), base_line + timedelta(minutes=30)
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            cache = session.time_series_by_doc_id.get(document_id)
            ranges = cache.get("BloodPressure")
            self.assertEqual(1, len(ranges))
            self.assertEqual(0, len(ranges[0].entries))

            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=30), ranges[0].to_date)
