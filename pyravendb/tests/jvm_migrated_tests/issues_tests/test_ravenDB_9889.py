from pyravendb.documents.session.event_args import (
    BeforeConversionToDocumentEventArgs,
    AfterConversionToDocumentEventArgs,
)
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.tests.test_base import TestBase


class Item:
    def __init__(self, Id: str = None, before: bool = None, after: bool = None):
        self.Id = Id
        self.before = before
        self.after = after


class TestRavenDB9889(TestBase):
    def setUp(self):
        super(TestRavenDB9889, self).setUp()

    def test_can_use_to_document_conversion_events(self):
        def __before_callback(args: BeforeConversionToDocumentEventArgs):
            if isinstance(args.entity, Item):
                args.entity.before = True

        def __after_callback(args: AfterConversionToDocumentEventArgs):
            if isinstance(args.entity, Item):
                args.document["after"] = True
                args.entity.after = True

        self.store.add_before_conversion_to_document(__before_callback)
        self.store.add_after_conversion_to_document(__after_callback)

        with self.store.open_session() as session:
            session.store(Item(), "items/1")
            session.save_changes()

            self.assertEqual(1, session.number_of_requests)
            session.save_changes()

            self.assertEqual(1, session.number_of_requests)

        with self.store.open_session() as session:
            item = session.load("items/1", Item)
            self.assertIsNotNone(item)
            self.assertTrue(item.before)
            self.assertTrue(item.after)
