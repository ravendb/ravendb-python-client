from __future__ import annotations
from datetime import datetime
from typing import List, Any, Dict

from ravendb import PatchRequest, PatchOperation
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


class CustomType:
    def __init__(
        self, Id: str = None, owner: str = None, value: int = None, comments: List[str] = None, date: datetime = None
    ):
        self.Id = Id
        self.owner = owner
        self.value = value
        self.comments = comments
        self.date = date

    def to_json(self) -> Dict[str, Any]:
        return {
            "Id": self.Id,
            "owner": self.owner,
            "value": self.value,
            "comments": self.comments,
            "date": Utils.datetime_to_string(self.date),
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CustomType:
        return cls(
            json_dict["Id"],
            json_dict["owner"],
            json_dict["value"],
            json_dict["comments"],
            Utils.string_to_datetime(json_dict["date"]),
        )


class TestAdvancedPatching(TestBase):
    def setUp(self):
        super().setUp()

    def test_with_variables(self):
        with self.store.open_session() as session:
            custom_type = CustomType()
            custom_type.owner = "me"
            session.store(custom_type, "customTypes/1-A")
            session.save_changes()

        patch_request = PatchRequest()
        patch_request.script = "this.owner = args.v1"
        patch_request.values = {"v1": "not-me"}

        patch_operation = PatchOperation("customTypes/1-A", None, patch_request)
        self.store.operations.send(patch_operation)

        with self.store.open_session() as session:
            loaded = session.load("customTypes/1-A", CustomType)
            self.assertEqual("not-me", loaded.owner)
