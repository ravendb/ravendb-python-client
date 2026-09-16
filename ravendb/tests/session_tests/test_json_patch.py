"""
Tests for the session patch methods emitting JsonPatch commands in place of a JavaScript
patch, and for the cases that keep them on the JavaScript path.
"""

import unittest
from datetime import datetime, timedelta

from ravendb import DocumentStore
from ravendb.documents.conventions import DocumentConventions, SessionPatchBehavior
from ravendb.documents.operations.patch import PatchStatus
from ravendb.documents.session.document_session import DocumentSession
from ravendb.tests.test_base import TestBase

_to_pointer = DocumentSession._Advanced._to_json_pointer
_can_patch = DocumentSession._Advanced._can_json_patch_value

DOCUMENT = {
    "Name": "original",
    "Tags": ["a", "b"],
    "Counts": {"x": 1},
    "Address": {"City": "Warsaw"},
    "@metadata": {"@collection": "Tests"},
}


class Thing:
    def __init__(self, Name: str = None, Tags: list = None):
        self.Name = Name
        self.Tags = Tags


def _deferred(session):
    return [command.command_type.value for command in session._deferred_commands]


class TestPathToJsonPointer(unittest.TestCase):
    def test_a_plain_member(self):
        self.assertEqual("/Name", _to_pointer("Name"))

    def test_a_nested_member(self):
        self.assertEqual("/Address/City", _to_pointer("Address.City"))

    def test_an_array_index(self):
        self.assertEqual("/Tags/0", _to_pointer("Tags[0]"))

    def test_an_index_in_the_middle(self):
        self.assertEqual("/Orders/2/Lines/0/Price", _to_pointer("Orders[2].Lines[0].Price"))

    def test_consecutive_indexers(self):
        self.assertEqual("/Grid/1/2", _to_pointer("Grid[1][2]"))

    def test_a_member_needing_escaping(self):
        self.assertEqual("/a~1b", _to_pointer("a/b"))

    def test_paths_json_patch_cannot_address(self):
        # Each of these sends the caller back to the JavaScript patch.
        for path in ("", "   ", "a..b", "Tags[]", "Tags[x]", "Tags[0", "[0]", "Tags[0]junk"):
            self.assertIsNone(_to_pointer(path), path)


class TestValuesJsonPatchCanCarry(unittest.TestCase):
    def test_json_scalars_are_fine(self):
        for value in (None, "text", True, 1, 1.5):
            self.assertTrue(_can_patch(value), repr(value))

    def test_so_is_anything_the_conventions_render_as_one_scalar(self):
        for value in (datetime.now(), timedelta(minutes=5), PatchStatus.PATCHED):
            self.assertTrue(_can_patch(value), repr(value))

    def test_anything_richer_stays_on_javascript(self):
        # The JavaScript path already knows how to serialize these, so it keeps them.
        for value in ({"a": 1}, ["a"], object(), (1, 2)):
            self.assertFalse(_can_patch(value), repr(value))


class TestSessionPatchBehaviorConvention(unittest.TestCase):
    def test_json_patch_is_the_default(self):
        self.assertEqual(SessionPatchBehavior.JSON_PATCH, DocumentConventions().session_patch_behavior)

    def test_it_can_be_turned_off(self):
        conventions = DocumentConventions()
        conventions.session_patch_behavior = SessionPatchBehavior.JAVA_SCRIPT

        self.assertEqual(SessionPatchBehavior.JAVA_SCRIPT, conventions.session_patch_behavior)

    def test_a_clone_keeps_it(self):
        # The request executor works off a clone, which is what the session reads.
        conventions = DocumentConventions()
        conventions.session_patch_behavior = SessionPatchBehavior.JAVA_SCRIPT

        self.assertEqual(SessionPatchBehavior.JAVA_SCRIPT, conventions.clone().session_patch_behavior)


class TestSessionEmitsJsonPatch(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as session:
            session.store(dict(DOCUMENT), "docs/1")
            session.save_changes()

    def test_a_scalar_patch_becomes_a_json_patch(self):
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Name", "patched")
            self.assertEqual(["JsonPatch"], _deferred(session))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual("patched", session.load("docs/1", dict)["Name"])

    def test_a_nested_path_and_an_array_index(self):
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Address.City", "Krakow")
            session.advanced.patch("docs/1", "Tags[0]", "z")
            session.save_changes()

        with self.store.open_session() as session:
            document = session.load("docs/1", dict)
            self.assertEqual("Krakow", document["Address"]["City"])
            self.assertEqual(["z", "b"], document["Tags"])

    def test_patches_for_one_document_merge_into_a_single_command(self):
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Name", "one")
            session.advanced.patch("docs/1", "Address.City", "two")

            self.assertEqual(["JsonPatch"], _deferred(session))
            self.assertEqual(2, len(session._deferred_commands[0].json_patch))

    def test_an_array_adder_appends(self):
        with self.store.open_session() as session:
            session.advanced.patch_array("docs/1", "Tags", lambda array: array.add("c").add("d"))
            self.assertEqual(["JsonPatch"], _deferred(session))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(["a", "b", "c", "d"], session.load("docs/1", dict)["Tags"])

    def test_an_array_adder_removes_by_index(self):
        with self.store.open_session() as session:
            session.advanced.patch_array("docs/1", "Tags", lambda array: array.remove_at(0))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(["b"], session.load("docs/1", dict)["Tags"])

    def test_a_map_adder_puts_and_removes(self):
        with self.store.open_session() as session:
            session.advanced.patch_object("docs/1", "Counts", lambda m: m.put("y", 2))
            self.assertEqual(["JsonPatch"], _deferred(session))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual({"x": 1, "y": 2}, session.load("docs/1", dict)["Counts"])

        with self.store.open_session() as session:
            session.advanced.patch_object("docs/1", "Counts", lambda m: m.remove("x"))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual({"y": 2}, session.load("docs/1", dict)["Counts"])

    def test_a_datetime_lands_exactly_as_the_javascript_path_wrote_it(self):
        # A value the conventions stringify has to serialize the same on both paths,
        # otherwise switching the default would rewrite what already-stored documents hold.
        stamp = datetime(2026, 9, 15, 10, 30, 45, 123400)

        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Stamp", stamp)
            self.assertEqual(["JsonPatch"], _deferred(session))
            session.save_changes()

        with self.store.open_session() as session:
            via_json_patch = session.load("docs/1", dict)["Stamp"]

        with DocumentStore(self.store.urls, self.store.database) as store:
            store.conventions.session_patch_behavior = SessionPatchBehavior.JAVA_SCRIPT
            store.initialize()

            with store.open_session() as session:
                session.store(dict(DOCUMENT), "docs/2")
                session.save_changes()

            with store.open_session() as session:
                session.advanced.patch("docs/2", "Stamp", stamp)
                self.assertEqual(["PATCH"], _deferred(session))
                session.save_changes()

            with store.open_session() as session:
                self.assertEqual(via_json_patch, session.load("docs/2", dict)["Stamp"])

    def test_a_tracked_entity_is_refreshed_from_the_result(self):
        with self.store.open_session() as session:
            session.store(Thing("original", ["a", "b"]), "things/1")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load("things/1", Thing)
            session.advanced.patch("things/1", "Name", "refreshed")
            session.save_changes()

            self.assertEqual("refreshed", loaded.Name)


class TestSessionFallsBackToJavaScript(TestBase):
    def setUp(self):
        super().setUp()
        with self.store.open_session() as session:
            session.store(dict(DOCUMENT), "docs/1")
            session.save_changes()

    def test_a_value_json_patch_cannot_carry(self):
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Address", {"City": "Gdansk"})

            self.assertEqual(["PATCH"], _deferred(session))
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual("Gdansk", session.load("docs/1", dict)["Address"]["City"])

    def test_an_array_predicate_json_patch_cannot_express(self):
        with self.store.open_session() as session:
            session.advanced.patch_array("docs/1", "Tags", lambda array: array.remove_all("item == 'a'"))

            self.assertEqual(["PATCH"], _deferred(session))

    def test_a_path_json_patch_cannot_address(self):
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Tags[x]", "z")

            self.assertEqual(["PATCH"], _deferred(session))

    def test_a_javascript_patch_already_deferred_keeps_the_rest_on_javascript(self):
        # Mixing both command types for one document would apply them out of order.
        with self.store.open_session() as session:
            session.advanced.patch("docs/1", "Address", {"City": "Gdansk"})
            session.advanced.patch("docs/1", "Name", "also js")

            self.assertEqual(["PATCH"], _deferred(session))

    def test_increment_stays_on_javascript(self):
        # JsonPatch has no read-modify-write operation.
        with self.store.open_session() as session:
            session.advanced.increment("docs/1", "Counts.x", 5)

            self.assertEqual(["PATCH"], _deferred(session))


class TestSessionPatchOptedOut(TestBase):
    def test_the_convention_turns_json_patch_off_entirely(self):
        with DocumentStore(self.store.urls, self.store.database) as store:
            store.conventions.session_patch_behavior = SessionPatchBehavior.JAVA_SCRIPT
            store.initialize()

            with store.open_session() as session:
                session.store(dict(DOCUMENT), "opted/1")
                session.save_changes()

            with store.open_session() as session:
                session.advanced.patch("opted/1", "Name", "via javascript")
                session.advanced.patch_array("opted/1", "Tags", lambda array: array.add("c"))

                self.assertEqual(["PATCH"], _deferred(session))
                session.save_changes()

            with store.open_session() as session:
                document = session.load("opted/1", dict)
                self.assertEqual("via javascript", document["Name"])
                self.assertEqual(["a", "b", "c"], document["Tags"])
