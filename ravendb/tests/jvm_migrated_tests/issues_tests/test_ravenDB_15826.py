from typing import List

from ravendb.tests.test_base import TestBase


class Item:
    def __init__(self, refs: List[str] = None):
        self.refs = refs or []


class TestRavenDB15826(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_include_lazy_load_item_that_is_already_on_session(self):
        with self.store.open_session() as session:
            session.store(Item(), "items/a")
            session.store(Item(), "items/b")
            item_c = Item(["items/a", "items/b"])
            session.store(item_c, "items/c")
            item_d = Item(["items/a"])
            session.store(item_d, "items/d")
            session.save_changes()

        with self.store.open_session() as session:
            session.include("refs").load("items/d", Item)  # include, some loaded
            a: Item = session.load("items/c", Item)
            items = session.advanced.lazily.load(a.refs, Item)
            session.advanced.eagerly.execute_all_pending_lazy_operations()
            items_map = items.value
            self.assertEqual(len(items_map), len(a.refs))
            self.assertEqual(0, len(list(filter(lambda x: x is None, items_map.values()))))
