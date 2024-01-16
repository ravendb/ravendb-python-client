from __future__ import annotations
from datetime import datetime
from typing import List, Any, Dict

from ravendb import (
    PatchRequest,
    PatchOperation,
    PatchStatus,
    IndexDefinition,
    PutIndexesOperation,
    PatchByQueryOperation,
)
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils

_SAMPLE_SCRIPT = (
    "this.comments.splice(2, 1);\n"
    "    this.owner = 'Something new';\n"
    "    this.value++;\n"
    '    this.newValue = "err!!";\n'
    "    this.comments = this.comments.map(function(comment) {\n"
    '        return (comment == "one") ? comment + " test" : comment;\n'
    "    });"
)


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

    def test_can_apply_basic_script_as_patch(self):
        with self.store.open_session() as session:
            test = CustomType("someId", "bob", 12143, ["one", "two", "seven"])
            session.store(test)
            session.save_changes()

        self.store.operations.send(PatchOperation("someId", None, PatchRequest.for_script(_SAMPLE_SCRIPT)))

        with self.store.open_session() as session:
            result = session.load("someId", CustomType)

            self.assertEqual("Something new", result.owner)
            self.assertEqual(2, len(result.comments))
            self.assertEqual("one test", result.comments[0])
            self.assertEqual("two", result.comments[1])
            self.assertEqual(12144, result.value)

    def test_can_deserialize_modified_document(self):
        custom_type = CustomType(owner="somebody@somewhere.com")
        with self.store.open_session() as session:
            session.store(custom_type, "doc")
            session.save_changes()

        patch1 = PatchOperation("doc", None, PatchRequest.for_script("this.owner = '123';"))

        result = self.store.operations.send_patch_operation_with_entity_class(CustomType, patch1)

        self.assertEqual(PatchStatus.PATCHED, result.status)
        self.assertEqual("123", result.document.owner)

        patch2 = PatchOperation("doc", None, PatchRequest.for_script("this.owner = '123';"))

        result = self.store.operations.send_patch_operation_with_entity_class(CustomType, patch2)

        self.assertEqual(PatchStatus.NOT_MODIFIED, result.status)
        self.assertEqual("123", result.document.owner)

    def test_can_create_documents_if_patching_applied_by_index(self):
        with self.store.open_session() as new_session:
            type1 = CustomType(Id="Item/1")
            type1.value = 1

            type2 = CustomType(Id="Item/2")
            type2.value = 2

            new_session.store(type1)
            new_session.store(type2)
            new_session.save_changes()

        def1 = IndexDefinition()
        def1.name = "TestIndex"
        def1.maps = {"from doc in docs.CustomTypes select new { doc.value }"}

        self.store.maintenance.send(PutIndexesOperation(def1))

        with self.store.open_session() as session:
            list(session.advanced.document_query("TestIndex", None, CustomType, False).wait_for_non_stale_results())

        operation = self.store.operations.send_async(
            PatchByQueryOperation(
                "FROM INDEX 'TestIndex' WHERE value = 1 update { put('NewItem/3', {'copiedValue': this.value });}"
            )
        )

        operation.wait_for_completion()

        with self.store.open_session() as session:
            json_document = session.load("NewItem/3", dict)
            self.assertEqual(1.0, json_document.get("copiedValue"))
