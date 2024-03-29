from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict

from ravendb import AbstractIndexCreationTask
from ravendb.documents.queries.time_series import TimeSeriesAggregationResult, TimeSeriesRawResult
from ravendb.tests.test_base import TestBase

document_id = "users/ayende"
base_line = datetime(2023, 8, 20, 21, 00)
base_line2 = base_line - timedelta(days=1)
ts_name_1 = "Heartrate"
ts_name_2 = "BloodPressure"
tag1 = "watches/fitbit"
tag2 = "watches/apple"


class PeopleIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(PeopleIndex, self).__init__()
        self.map = "from p in docs.People select new { p.age }"

    @property
    def index_name(self) -> str:
        return "People"


class Event:
    def __init__(self, start: datetime = None, end: datetime = None, description: str = None):
        self.start = start
        self.end = end
        self.description = description

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Event:
        return cls(json_dict["start"], json_dict["end"], json_dict["description"])


class NestedClass:
    def __init__(self, event: Event = None, accuracy: float = None):
        self.event = event
        self.accuracy = accuracy

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> NestedClass:
        return cls(Event.from_json(json_dict["event"]), json_dict["accuracy"])


class AdditionalData:
    def __init__(self, nested_class: NestedClass = None):
        self.nested_class = nested_class

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AdditionalData:
        return cls(NestedClass.from_json(json_dict["nested_class"]))


class Person:
    def __init__(
        self,
        name: str = None,
        age: int = None,
        works_at: str = None,
        event: str = None,
        additional_data: AdditionalData = None,
    ):
        self.name = name
        self.age = age
        self.works_at = works_at
        self.event = event
        self.additional_data = additional_data

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Person:
        return cls(
            json_dict["name"],
            json_dict["age"],
            json_dict["works_at"],
            json_dict["event"],
            AdditionalData.from_json(json_dict["additional_data"]),
        )


class RawQueryResult:
    def __init__(
        self,
        heart_rate: TimeSeriesAggregationResult = None,
        blood_pressure: TimeSeriesAggregationResult = None,
        name: str = None,
    ):
        self.heart_rate = heart_rate
        self.blood_pressure = blood_pressure
        self.name = name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RawQueryResult:
        return cls(
            TimeSeriesAggregationResult.from_json(json_dict["heart_rate"]) if "heart_rate" in json_dict else None,
            (
                TimeSeriesAggregationResult.from_json(json_dict["blood_pressure"])
                if "blood_pressure" in json_dict
                else None
            ),
            json_dict["name"] if "name" in json_dict else None,
        )


class TestTimeSeriesRawQuery(TestBase):
    def setUp(self):
        super(TestTimeSeriesRawQuery, self).setUp()

    def test_can_query_time_series_aggregation_declare_syntax_multiple_series(self):
        with self.store.open_session() as session:
            for i in range(4):
                id_ = f"people/{i}"
                person = Person("Oren", i * 30)

                session.store(person, id_)

                tsf = session.time_series_for(id_, ts_name_1)

                tsf.append_single(base_line + timedelta(minutes=61), 59, tag1)
                tsf.append_single(base_line + timedelta(minutes=62), 79, tag1)
                tsf.append_single(base_line + timedelta(minutes=63), 69, tag1)

                tsf = session.time_series_for(id_, ts_name_2)

                tsf.append_single(base_line + timedelta(minutes=61), 159, tag2)
                tsf.append_single(base_line + timedelta(minutes=62), 179, tag2)
                tsf.append_single(base_line + timedelta(minutes=63), 168, tag2)

                session.save_changes()

        PeopleIndex().execute(self.store)
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out(p)\n "
                    "{\n"
                    "    from p.heartRate between $start and $end\n"
                    "    group by 1h\n"
                    "    select min(), max()\n"
                    "}\n"
                    "from index 'People' as p\n"
                    "where p.age > 49\n"
                    "select out(p) as heart_rate, p.name",
                    RawQueryResult,
                )
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=1))
            )

            result = list(query)

            self.assertEqual(2, len(result))

            for i in range(2):
                agg = result[i]
                self.assertEqual("Oren", agg.name)

                heartrate = agg.heart_rate

                self.assertEqual(3, heartrate.count)

                self.assertEqual(1, len(heartrate.results))

                val = heartrate.results[0]
                self.assertEqual(59, val.min[0])
                self.assertEqual(79, val.max[0])

                self.assertEqual(base_line + timedelta(minutes=60), val.from_date)
                self.assertEqual(base_line + timedelta(minutes=120), val.to_date)

    def test_can_query_time_series_aggregation_no_select_or_group_by_multiple_values(self):
        with self.store.open_session() as session:
            for i in range(4):
                id_ = f"people/{i}"
                person = Person("Oren", i * 30)

                session.store(person, id_)

                tsf = session.time_series_for(id_, ts_name_1)

                tsf.append(base_line + timedelta(minutes=61), [59, 159], tag1)
                tsf.append(base_line + timedelta(minutes=62), [79, 179], tag1)
                tsf.append_single(base_line + timedelta(minutes=63), 69, tag2)

                tsf.append(base_line + timedelta(days=31, minutes=61), [159, 259], tag1)
                tsf.append(base_line + timedelta(days=31, minutes=62), [179], tag2)
                tsf.append(base_line + timedelta(days=31, minutes=63), [169, 269], tag1)

                session.save_changes()

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out(x)\n"
                    "{\n"
                    "    from x.HeartRate between $start and $end\n"
                    "}\n"
                    "from People as doc\n"
                    "where doc.age > 49\n"
                    "select out(doc)"
                    "",
                    TimeSeriesRawResult,
                )
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=62))
            )

            result = list(query)
            self.assertEqual(2, len(result))

            for i in range(2):
                agg = result[i]
                self.assertEqual(6, len(agg.results))

                val = agg.results[0]

                self.assertEqual(2, len(val.values))
                self.assertEqual(59, val.values[0])
                self.assertEqual(159, val.values[1])

                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=61), val.timestamp)

                val = agg.results[1]

                self.assertEqual(2, len(val.values))
                self.assertEqual(79, val.values[0])
                self.assertEqual(179, val.values[1])

                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=62), val.timestamp)

                val = agg.results[2]

                self.assertEqual(1, len(val.values))
                self.assertEqual(69, val.values[0])

                self.assertEqual(tag2, val.tag)
                self.assertEqual(base_line + timedelta(minutes=63), val.timestamp)

                val = agg.results[3]

                self.assertEqual(2, len(val.values))
                self.assertEqual(159, val.values[0])
                self.assertEqual(259, val.values[1])

                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=61, days=31), val.timestamp)

                val = agg.results[4]

                self.assertEqual(1, len(val.values))
                self.assertEqual(179, val.values[0])

                self.assertEqual(tag2, val.tag)
                self.assertEqual(base_line + timedelta(minutes=62, days=31), val.timestamp)

                val = agg.results[5]

                self.assertEqual(2, len(val.values))
                self.assertEqual(169, val.values[0])
                self.assertEqual(269, val.values[1])

                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=63, days=31), val.timestamp)

    def test_can_query_time_series_aggregation_declare_syntax_with_other_fields(self):
        with self.store.open_session() as session:
            for i in range(4):
                id_ = f"people/{i}"
                person = Person("Oren", i * 30)

                session.store(person, id_)

                tsf = session.time_series_for(id_, ts_name_1)

                tsf.append_single(base_line + timedelta(minutes=61), 59, tag1)
                tsf.append_single(base_line + timedelta(minutes=62), 79, tag1)
                tsf.append_single(base_line + timedelta(minutes=63), 69, tag1)

                session.save_changes()

        PeopleIndex().execute(self.store)
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out(p)\n"
                    "{\n"
                    "    from p.HeartRate between $start and $end\n"
                    "    group by 1h\n"
                    "    select min(), max()\n"
                    "}\n"
                    "from index 'People' as p\n"
                    "where p.age > 49\n"
                    "select out(p) as heart_rate, p.name",
                    RawQueryResult,
                )
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=1))
            )

            result = list(query)

            self.assertEqual(2, len(result))

            for i in range(2):
                agg = result[i]
                self.assertEqual("Oren", agg.name)
                heartrate = agg.heart_rate

                self.assertEqual(3, heartrate.count)
                self.assertEqual(1, len(heartrate.results))

                val = heartrate.results[0]
                self.assertEqual(59, val.min[0])
                self.assertEqual(79, val.max[0])

                self.assertEqual(base_line + timedelta(minutes=60), val.from_date)
                self.assertEqual(base_line + timedelta(minutes=120), val.to_date)
