from __future__ import annotations
import datetime
from typing import Optional, Dict, List

from ravendb import AbstractIndexCreationTask, QueryStatistics
from ravendb.documents.indexes.definitions import FieldStorage
from ravendb.tests.test_base import TestBase


class MyDocumentItem:
    def __init__(self, date: datetime.datetime = None, latitude: float = None, longitude: float = None):
        self.date = date
        self.latitude = latitude
        self.longitude = longitude

    @classmethod
    def from_json(cls, json_dict: Dict) -> MyDocumentItem:
        return cls(datetime.datetime.fromisoformat(json_dict["date"]), json_dict["latitude"], json_dict["longitude"])

    def to_json(self) -> Dict:
        return {"date": self.date.isoformat(), "latitude": self.latitude, "longitude": self.longitude}


class MyDocument:
    def __init__(self, Id: str = None, items: List[MyDocumentItem] = None):
        self.Id = Id
        self.items = items

    @classmethod
    def from_json(cls, json_dict: Dict) -> MyDocument:
        return cls(json_dict["Id"], [MyDocumentItem.from_json(item) for item in json_dict["items"]])

    def to_json(self) -> Dict:
        return {"Id": self.Id, "items": [item.to_json() for item in self.items]}


class MyProjection:
    def __init__(self, Id: str = None, date: datetime.datetime = None, latitude: float = None, longitude: float = None):
        self.Id = Id
        self.date = date
        self.latitude = latitude
        self.longitude = longitude

    @classmethod
    def from_json(cls, json_dict: Dict) -> MyProjection:
        return cls(
            json_dict["Id"],
            datetime.datetime.fromisoformat(json_dict["date"]) if "date" in json_dict else None,
            json_dict["latitude"],
            json_dict["longitude"],
        )

    def to_json(self) -> Dict:
        return {"Id": self.Id, "date": self.date.isoformat(), "latitude": self.latitude, "longitude": self.longitude}


class MyIndex(AbstractIndexCreationTask):
    def __init__(self):
        super(MyIndex, self).__init__()
        self.map = (
            "docs.MyDocuments.SelectMany(doc => doc.items, (doc, item) => new {\n"
            "    doc = doc,\n"
            "    item = item\n"
            "}).Select(this0 => new {\n"
            "    this0 = this0,\n"
            "    lat = ((double)(this0.item.latitude ?? 0))\n"
            "}).Select(this1 => new {\n"
            "    this1 = this1,\n"
            "    lng = ((double)(this1.this0.item.longitude ?? 0))\n"
            "}).Select(this2 => new {\n"
            "    id = Id(this2.this1.this0.doc),\n"
            "    date = this2.this1.this0.item.date,\n"
            "    latitude = this2.this1.lat,\n"
            "    longitude = this2.lng,\n"
            "    coordinates = this.CreateSpatialField(((double ? ) this2.this1.lat), ((double ? ) this2.lng))\n"
            "})"
        )

        self._store("Id", FieldStorage.YES)
        self._store("date", FieldStorage.YES)

        self._store("latitude", FieldStorage.YES)
        self._store("longitude", FieldStorage.YES)


class TestSpatial(TestBase):
    def setUp(self):
        super(TestSpatial, self).setUp()

    def test_weird_spatial_results(self):
        with self.store.open_session() as session:
            my_document = MyDocument("First")
            my_document_item = MyDocumentItem(datetime.datetime.now(), 10.0, 10.0)
            my_document.items = [my_document_item]
            session.store(my_document)
            session.save_changes()

        MyIndex().execute(self.store)

        with self.store.open_session() as session:
            statistics: Optional[QueryStatistics] = None

            def __statistics_callback(stats: QueryStatistics):
                nonlocal statistics
                statistics = stats  # plug-in the reference, value will be changed

            result = list(
                session.advanced.document_query_from_index_type(MyIndex, MyDocument)
                .wait_for_non_stale_results()
                .within_radius_of("coordinates", 0, 12.3456789, 12.3456789)
                .statistics(__statistics_callback)
                .select_fields(MyProjection, "Id", "latitude", "longitude")
                .take(50)
            )

            self.assertEqual(0, statistics.total_results)
            self.assertEqual(0, len(result))

    def test_match_spatial_results(self):
        with self.store.open_session() as session:
            my_document = MyDocument("Firts")
            my_document_item = MyDocumentItem(datetime.datetime.now(), 10.0, 10.0)
            my_document.items = [my_document_item]
            session.store(my_document)
            session.save_changes()

        MyIndex().execute(self.store)

        with self.store.open_session() as session:
            query_stats: Optional[QueryStatistics] = None

            def _stats_callback(stats: QueryStatistics):
                nonlocal query_stats
                query_stats = stats

            result = list(
                session.advanced.document_query_from_index_type(MyIndex, MyDocument)
                .wait_for_non_stale_results()
                .within_radius_of("coordinates", 1, 10, 10)
                .statistics(_stats_callback)
                .select_fields(MyProjection, "Id", "latitude", "longitude")
                .take(50)
            )

            self.assertEqual(1, query_stats.total_results)
            self.assertEqual(1, len(result))
