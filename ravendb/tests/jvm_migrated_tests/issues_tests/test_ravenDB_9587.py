from ravendb.documents.queries.query import QueryTimings
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB9587(TestBase):
    def setUp(self):
        super(TestRavenDB9587, self).setUp()

    def test_timings_should_work(self):
        with self.store.open_session() as session:
            c1 = Company(name="CF")
            c2 = Company(name="HR")
            session.store(c1)
            session.store(c2)
            session.save_changes()

        with self.store.open_session() as session:
            timings: QueryTimings = None

            def timings_callback(timings_ref: QueryTimings):
                nonlocal timings
                timings = timings_ref

            companies = list(
                session.query(object_type=Company).timings(timings_callback).where_not_equals("name", "HR")
            )
            self.assertGreater(timings.duration_in_ms, 0)
            self.assertIsNotNone(timings.timings)
