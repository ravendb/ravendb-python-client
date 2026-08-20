import json
import math
import unittest
from datetime import datetime, timedelta
from enum import Enum, IntEnum

from ravendb.documents.conventions import DocumentConventions
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.tools.utils import Utils


class Color(Enum):
    RED = "red"


class Priority(IntEnum):
    HIGH = 9


class StringBacked(str, Enum):
    ALPHA = "alpha"


class Address:
    def __init__(self, city: str, zip_code: str):
        self.city = city
        self.zip_code = zip_code


class Person:
    def __init__(self, name: str, address: Address = None):
        self.name = name
        self.address = address


class WithToJson:
    def to_json(self):
        return {"rendered": 42}


class SelfReferencing:
    def __init__(self):
        self.child = None


class TestEntityToDictSemantics(unittest.TestCase):
    """Utils.entity_to_dict decides what a document looks like for both the session and bulk insert."""

    def setUp(self):
        self.default_method = DocumentConventions.json_default

    def test_conversion_matches_a_round_trip_through_a_json_string(self):
        moment = datetime(2026, 8, 19, 14, 30, 15)
        span = timedelta(days=2, minutes=23, seconds=59, milliseconds=254)
        cases = {
            "nested objects": (
                Person("Ayende", Address("Hadera", "38100")),
                {"name": "Ayende", "address": {"city": "Hadera", "zip_code": "38100"}},
            ),
            "to_json wins over the instance dict": ({"item": WithToJson()}, {"item": {"rendered": 42}}),
            "datetime": ({"at": moment}, {"at": Utils.datetime_to_string(moment)}),
            "timedelta": ({"took": span}, {"took": Utils.timedelta_to_str(span)}),
            "enum becomes its value": ({"color": Color.RED}, {"color": "red"}),
            "int enum becomes an int": ({"priority": Priority.HIGH}, {"priority": 9}),
            "str backed enum becomes a str": ({"kind": StringBacked.ALPHA}, {"kind": "alpha"}),
            "tuple becomes a list": ({"point": (1, 2, 3)}, {"point": [1, 2, 3]}),
            "set becomes a list": ({"tags": {"a"}}, {"tags": ["a"]}),
            "metadata as dictionary": (
                {"meta": MetadataAsDictionary({"@collection": "People"})},
                {"meta": {"@collection": "People"}},
            ),
            "scalar dict keys": ({1: "int", 1.5: "float", None: "none"}, {"1": "int", "1.5": "float", "null": "none"}),
            "bool dict keys": ({True: "yes", False: "no"}, {"true": "yes", "false": "no"}),
            "types are kept": (
                {"yes": True, "i": 7, "f": 1.25, "nothing": None},
                {"yes": True, "i": 7, "f": 1.25, "nothing": None},
            ),
            "non ascii text": ({"city": "Zurich, Kraków, 東京"}, {"city": "Zurich, Kraków, 東京"}),
            "objects in a list": (
                {"people": [Person("A", Address("Hadera", "1")), Person("B")]},
                {
                    "people": [
                        {"name": "A", "address": {"city": "Hadera", "zip_code": "1"}},
                        {"name": "B", "address": None},
                    ]
                },
            ),
            "the same object twice": (
                {"home": (shared := Address("Hadera", "38100")), "work": shared},
                {"home": {"city": "Hadera", "zip_code": "38100"}, "work": {"city": "Hadera", "zip_code": "38100"}},
            ),
            "dict ordering": ({"b": 1, "a": 2}, {"b": 1, "a": 2}),
        }
        for name, (value, expected) in cases.items():
            with self.subTest(name):
                converted = Utils.entity_to_dict(value, self.default_method)
                self.assertEqual(expected, converted)
                self.assertEqual(json.loads(json.dumps(value, default=self.default_method)), converted)
                if isinstance(expected, dict):
                    self.assertEqual(list(expected.keys()), list(converted.keys()))

    def test_non_finite_floats_survive_as_floats(self):
        converted = Utils.entity_to_dict({"nan": float("nan"), "inf": float("inf")}, self.default_method)
        self.assertTrue(math.isnan(converted["nan"]))
        self.assertEqual(float("inf"), converted["inf"])

    def test_circular_reference_is_reported_and_does_not_recurse_forever(self):
        first, second = SelfReferencing(), SelfReferencing()
        first.child, second.child = second, first
        with self.assertRaises(ValueError):
            Utils.entity_to_dict(first, self.default_method)

    def test_values_that_cannot_be_converted_are_rejected(self):
        for name, value in {
            "unsupported dict key": {(1, 2): "tuple key"},
            "opaque value": {"opaque": object()},
        }.items():
            with self.subTest(name), self.assertRaises(TypeError):
                Utils.entity_to_dict(value, self.default_method)


if __name__ == "__main__":
    unittest.main()
