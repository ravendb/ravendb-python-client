from __future__ import annotations
from typing import Dict

from ravendb.tests.test_base import TestBase
from datetime import datetime
from ravendb.tools.utils import Utils
import unittest


class Address:
    def __init__(self, street, city, state):
        self.street = street
        self.city = city
        self.state = state

    @classmethod
    def from_json(cls, json_dict: Dict) -> Address:
        return cls(json_dict["street"], json_dict["city"], json_dict["state"])


class Owner:
    def __init__(self, name, address):
        self.name = name
        self.address = address

    @classmethod
    def from_json(cls, json_dict: Dict) -> Owner:
        return cls(json_dict["name"], Address.from_json(json_dict["address"]))


class Dog:
    def __init__(self, name, owner, date=None):
        self.name = name
        self.owner = owner
        self.date = date if date else datetime.now()

    @classmethod
    def from_json(cls, json_dict: Dict) -> Dog:
        return cls(json_dict["name"], Owner.from_json(json_dict["owner"]), Utils.string_to_datetime(json_dict["date"]))


def get_dog(key, value):
    if key is None or value is None:
        return None

    if key == "address":
        return Address(**value)
    elif key == "owner":
        return Owner(**value)
    elif key == "date":
        return Utils.string_to_datetime(value)


class TestToJson(TestBase):
    def setUp(self):
        super(TestToJson, self).setUp()
        with self.store.open_session() as session:
            session.store(
                Dog(
                    name="Donald",
                    owner=Owner(name="Idan", address=Address("Ru", "Harish", "Israel")),
                ),
                key="Dogs/1-A",
            )
            session.store(
                Dog(
                    "Scooby Doo",
                    Owner("Shaggy ", Address("UnKnown", "UnKnown", "America")),
                ),
                key="Dogs/2-A",
            )
            session.store(
                Dog(
                    "Courage",
                    Owner(
                        "John R. Dilworth",
                        Address("Middle of Nowhere", "UnKnown", "America"),
                    ),
                ),
                key="Dogs/3-A",
            )
            session.save_changes()

    def tearDown(self):
        super(TestToJson, self).tearDown()
        self.delete_all_topology_files()

    def test_load_with_mappers(self):
        with self.store.open_session() as session:
            dog = session.load("Dogs/3-A", object_type=Dog)
            self.assertIsNotNone(dog)
            self._check_dog_type(dog)

    def test_query_with_mappers(self):
        with self.store.open_session() as session:
            results = list(session.query(object_type=Dog))
            self.assertIsNotNone(results)
            for result in results:
                self._check_dog_type(result)

    def test_multi_load_with_mappers(self):
        with self.store.open_session() as session:
            dogs = session.load(["Dogs/1-A", "Dogs/2-A", "Dogs/3-A"], object_type=Dog)
            self.assertIsNotNone(dogs)
            for dog in dogs.values():
                self._check_dog_type(dog)

    def test_date_time_convert(self):
        with self.store.open_session() as session:
            dog = session.load("Dogs/1-A", object_type=Dog)
            self.assertIsNotNone(dog)
            self.assertTrue(isinstance(dog.date, datetime))

    def _check_dog_type(self, obj):
        self.assertTrue(isinstance(obj, Dog))
        self.assertTrue(isinstance(obj.owner, Owner))
        self.assertTrue(isinstance(obj.owner.address, Address))


if __name__ == "__main__":
    unittest.main()
