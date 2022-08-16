import datetime
import time

from ravendb import DocumentStore, RefreshConfiguration
from ravendb.documents.operations.refresh.configuration import ConfigureRefreshOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Stopwatch, TimeUnit


class TestRavenDB13735(TestBase):
    def setUp(self):
        super(TestRavenDB13735, self).setUp()

    def _setup_refresh(self, store: DocumentStore) -> None:
        config = RefreshConfiguration()
        config.disabled = False
        config.refresh_frequency_in_sec = 1

        store.maintenance.send(ConfigureRefreshOperation(config))

    def test_refresh_will_update_document_change_vector(self):
        self._setup_refresh(self.store)

        expected_change_vector = None
        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/1-A")

            datetime_now = datetime.datetime.now()
            hour_ago = datetime.datetime(
                year=datetime_now.year,
                month=datetime_now.month,
                day=datetime_now.day,
                hour=datetime_now.hour - 1,
                minute=datetime_now.minute,
                second=datetime_now.second,
                microsecond=datetime_now.microsecond,
                tzinfo=datetime_now.tzinfo,
            )

            session.advanced.get_metadata_for(user)["@refresh"] = hour_ago.isoformat()
            session.save_changes()

            expected_change_vector = session.advanced.get_change_vector_for(user)

        sw = Stopwatch.create_started()

        while True:
            if sw.elapsed(TimeUnit.SECONDS) > 10:
                raise TimeoutError()

            with self.store.open_session() as session:
                user = session.load("users/1-A", User)
                self.assertIsNotNone(user)

                if not session.advanced.get_change_vector_for(user) == expected_change_vector:
                    # change vector was changed - great!
                    break

            time.sleep(0.2)
