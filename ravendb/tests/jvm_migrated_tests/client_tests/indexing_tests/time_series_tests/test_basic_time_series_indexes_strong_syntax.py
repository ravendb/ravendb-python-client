from __future__ import annotations
import datetime
from typing import Optional, Any, Dict

from ravendb.documents.indexes.time_series import (
    AbstractTimeSeriesIndexCreationTask,
    AbstractMultiMapTimeSeriesIndexCreationTask,
)
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper
from ravendb.tools.utils import Utils


class TestBasicTimeSeriesIndexes_StrongSyntax(TestBase):
    def setUp(self):
        super().setUp()

    def test_basic_map_index(self):
        now1 = RavenTestHelper.utc_today()

        with self.store.open_session() as session:
            company = Company()
            session.store(company, "companies/1")
            session.time_series_for_entity(company, "HeartRate").append_single(now1, 7, "tag")
            session.save_changes()

        time_series_index = MyTsIndex()
        index_definition = time_series_index.create_index_definition()

        self.assertIsNotNone(index_definition)
        time_series_index.execute(self.store)

        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(session.query_index_type(MyTsIndex, MyMultiMapTsIndex.Result))

            self.assertEqual(1, len(results))

            result = results[0]

            self.assertEqual(now1, result.date)
            self.assertIsNotNone(result.user)
            self.assertGreater(result.heart_beat, 0)


class MyTsIndex(AbstractTimeSeriesIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from ts in timeSeries.Companies.HeartRate "
            "from entry in ts.Entries "
            "select new { "
            "   heartBeat = entry.Values[0], "
            "   date = entry.Timestamp.Date, "
            "   user = ts.DocumentId "
            "}"
        )


class MyMultiMapTsIndex(AbstractMultiMapTimeSeriesIndexCreationTask):
    class Result:
        def __init__(
            self,
            heart_beat: Optional[float] = None,
            date: Optional[datetime.datetime] = None,
            user: Optional[str] = None,
        ):
            self.heart_beat = heart_beat
            self.date = date
            self.user = user

        @classmethod
        def from_json(cls, json_dict:Dict[str, Any]) -> MyMultiMapTsIndex.Result:
            return cls(
                json_dict["heartBeat"],
                Utils.string_to_datetime(json_dict["date"]),
                json_dict["user"]
            )


    def __init__(self):
        super(MyMultiMapTsIndex, self).__init__()
        self._add_map(
            "from ts in timeSeries.Companies.HeartRate "
            + "from entry in ts.Entries "
            + "select new { "
            + "   heartBeat = entry.Values[0], "
            + "   date = entry.Timestamp.Date, "
            + "   user = ts.DocumentId "
            + "}"
        )

        self._add_map(
            "from ts in timeSeries.Companies.HeartRate2 "
            + "from entry in ts.Entries "
            + "select new { "
            + "   heartBeat = entry.Values[0], "
            + "   date = entry.Timestamp.Date, "
            + "   user = ts.DocumentId "
            + "}"
        )

        self._add_map(
            "from ts in timeSeries.Users.HeartRate "
            + "from entry in ts.Entries "
            + "select new { "
            + "   heartBeat = entry.Values[0], "
            + "   date = entry.Timestamp.Date, "
            + "   user = ts.DocumentId "
            + "}"
        )
