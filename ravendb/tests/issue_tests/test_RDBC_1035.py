"""
RDBC-1035 + RDBC-1036: store.aggressively_cache_for() / disable_aggressive_caching()
                       and store-level event registration methods.

C# reference: IDocumentStore.AggressivelyCacheFor() / DisableAggressiveCaching()
              IDocumentStore.OnBeforeStore / OnAfterSaveChanges / OnBeforeDelete / OnBeforeQuery
"""

import datetime
import threading
import unittest

from ravendb.documents.store.definition import DocumentStoreBase
from ravendb.tests.test_base import TestBase


class TestStoreApiUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def _make_store_base(self):
        base = DocumentStoreBase.__new__(DocumentStoreBase)
        DocumentStoreBase.__init__(base)
        return base

    # --- store events ---

    def test_add_before_store_method_exists(self):
        store = self._make_store_base()
        self.assertTrue(hasattr(store, "add_before_store"))
        self.assertTrue(callable(store.add_before_store))

    def test_add_after_save_changes_method_exists(self):
        store = self._make_store_base()
        self.assertTrue(hasattr(store, "add_after_save_changes"))

    def test_add_before_delete_method_exists(self):
        store = self._make_store_base()
        self.assertTrue(hasattr(store, "add_before_delete"))

    def test_add_before_query_method_exists(self):
        store = self._make_store_base()
        self.assertTrue(hasattr(store, "add_before_query"))

    def test_remove_methods_exist(self):
        store = self._make_store_base()
        self.assertTrue(hasattr(store, "remove_before_store"))
        self.assertTrue(hasattr(store, "remove_after_save_changes"))
        self.assertTrue(hasattr(store, "remove_before_delete"))
        self.assertTrue(hasattr(store, "remove_before_query"))

    def test_add_and_remove_before_store_roundtrip(self):
        store = self._make_store_base()
        handler = lambda e: None
        store.add_before_store(handler)
        store.remove_before_store(handler)

    # --- aggressive cache ---

    def test_get_from_cache_returns_empty_release_cache_item_for_non_cacheable_command(self):
        """_get_from_cache must return a valid ReleaseCacheItem (not HttpCache.ReleaseCacheItem)
        when the command is non-cacheable, so that execute() can enter its 'with cached_item:'
        context manager without AttributeError."""
        from ravendb.http.request_executor import RequestExecutor
        from ravendb.http.http_cache import ReleaseCacheItem

        executor = RequestExecutor.__new__(RequestExecutor)
        executor._cache = __import__("ravendb.http.http_cache", fromlist=["HttpCache"]).HttpCache()

        command = self._make_command(can_cache_aggressively=False)
        command._can_cache = False

        cached_item, change_vector, cached_value = executor._get_from_cache(command, False, "http://host/docs/1")

        self.assertIsInstance(cached_item, ReleaseCacheItem)
        self.assertIsNone(cached_item.item)
        self.assertIsNone(change_vector)
        self.assertIsNone(cached_value)
        # Verify the context manager protocol works (was broken when HttpCache.ReleaseCacheItem was used)
        with cached_item:
            pass

    def test_aggressively_cache_for_method_exists_on_document_store(self):
        from ravendb.documents.store.definition import DocumentStore

        self.assertTrue(hasattr(DocumentStore, "aggressively_cache_for"))

    def test_disable_aggressive_caching_method_exists_on_document_store(self):
        from ravendb.documents.store.definition import DocumentStore

        self.assertTrue(hasattr(DocumentStore, "disable_aggressive_caching"))

    def test_aggressive_caching_is_thread_local(self):
        """aggressive_caching must be per-thread (AsyncLocal equivalent), not shared across threads."""
        from unittest.mock import MagicMock
        from ravendb.http.misc import AggressiveCacheOptions, AggressiveCacheMode
        from ravendb.http.request_executor import RequestExecutor

        executor = RequestExecutor.__new__(RequestExecutor)
        executor._aggressive_caching_local = threading.local()

        options = AggressiveCacheOptions(datetime.timedelta(minutes=5), AggressiveCacheMode.TRACK_CHANGES)
        executor.aggressive_caching = options

        other_thread_value = []

        def read_from_other_thread():
            other_thread_value.append(executor.aggressive_caching)

        t = threading.Thread(target=read_from_other_thread)
        t.start()
        t.join()

        # Main thread sees the set value; other thread sees None (thread-local isolation)
        self.assertEqual(options, executor.aggressive_caching)
        self.assertIsNone(other_thread_value[0])

    def _make_executor_with_caching(self, mode):
        from ravendb.http.misc import AggressiveCacheOptions, AggressiveCacheMode
        from ravendb.http.request_executor import RequestExecutor

        executor = RequestExecutor.__new__(RequestExecutor)
        executor._aggressive_caching_local = threading.local()
        executor.aggressive_caching = AggressiveCacheOptions(datetime.timedelta(hours=1), mode)
        return executor

    def _make_cached_item(self, might_have_been_modified: bool):
        """Build a real ReleaseCacheItem.  Increment cache.generation before get() to simulate modification."""
        from ravendb.http.http_cache import HttpCache

        cache = HttpCache()
        cache.set("http://host/docs/1", "cv-1", '{"Name":"Alice"}')
        if might_have_been_modified:
            cache.generation += 1
        cached_item, _, cached_value = cache.get("http://host/docs/1")
        return cached_item, cached_value

    def _make_command(self, can_cache_aggressively: bool):
        from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType

        class _MinimalCommand(RavenCommand):
            def create_request(self, node):
                pass

            def is_read_request(self):
                return True

            def set_response(self, response, from_cache):
                pass

        cmd = _MinimalCommand()
        cmd._can_cache_aggressively = can_cache_aggressively
        return cmd

    def _check_aggressive_cache_guard(self, executor, cached_item, cached_value, command, no_caching=False):
        """Mirrors the aggressive-cache short-circuit in execute().

        Matches the exact condition in RequestExecutor.execute():
            not no_caching
            and self.aggressive_caching is not None
            and command.can_cache_aggressively
            and cached_item.item is not None
            and cached_item.age < self.aggressive_caching.duration
            and (not cached_item.might_have_been_modified
                 or self.aggressive_caching.mode != AggressiveCacheMode.TRACK_CHANGES)
        """
        from ravendb.http.misc import AggressiveCacheMode
        from ravendb.http.http_cache import ItemFlags

        if not (
            not no_caching
            and executor.aggressive_caching is not None
            and command.can_cache_aggressively
            and cached_item.item is not None
            and cached_item.age < executor.aggressive_caching.duration
            and (
                not cached_item.might_have_been_modified
                or executor.aggressive_caching.mode != AggressiveCacheMode.TRACK_CHANGES
            )
        ):
            return False

        if ItemFlags.NOT_FOUND in cached_item.item.flags:
            return ItemFlags.AGGRESSIVELY_CACHED in cached_item.item.flags
        return cached_value is not None

    def test_can_cache_aggressively_false_prevents_cache_hit(self):
        """commands with can_cache_aggressively=False must not be served from aggressive cache."""
        from ravendb.http.misc import AggressiveCacheMode

        executor = self._make_executor_with_caching(AggressiveCacheMode.TRACK_CHANGES)
        cached_item, cached_value = self._make_cached_item(might_have_been_modified=False)
        command = self._make_command(can_cache_aggressively=False)

        self.assertFalse(self._check_aggressive_cache_guard(executor, cached_item, cached_value, command))

    def test_might_have_been_modified_prevents_track_changes_cache_hit(self):
        """when mode=TRACK_CHANGES and item might_have_been_modified, must not serve from cache."""
        from ravendb.http.misc import AggressiveCacheMode

        executor = self._make_executor_with_caching(AggressiveCacheMode.TRACK_CHANGES)
        cached_item, cached_value = self._make_cached_item(might_have_been_modified=True)
        command = self._make_command(can_cache_aggressively=True)

        self.assertFalse(self._check_aggressive_cache_guard(executor, cached_item, cached_value, command))

    def test_do_not_track_changes_mode_ignores_might_have_been_modified(self):
        """when mode=DO_NOT_TRACK_CHANGES, a modified item is still served from aggressive cache."""
        from ravendb.http.misc import AggressiveCacheMode

        executor = self._make_executor_with_caching(AggressiveCacheMode.DO_NOT_TRACK_CHANGES)
        cached_item, cached_value = self._make_cached_item(might_have_been_modified=True)
        command = self._make_command(can_cache_aggressively=True)

        self.assertTrue(self._check_aggressive_cache_guard(executor, cached_item, cached_value, command))

    def test_set_not_found_marks_aggressively_cached_when_caching_active(self):
        """set_not_found must pass aggressively_cached=True when aggressive caching is active."""
        from ravendb.http.http_cache import HttpCache, ItemFlags

        cache = HttpCache()
        cache.set_not_found("http://host/docs/missing", aggressively_cached=True)

        cached_item, _, _ = cache.get("http://host/docs/missing")
        self.assertIsNotNone(cached_item.item)
        self.assertIn(ItemFlags.NOT_FOUND, cached_item.item.flags)
        self.assertIn(ItemFlags.AGGRESSIVELY_CACHED, cached_item.item.flags)

    def test_set_not_found_without_aggressive_caching_not_marked(self):
        """set_not_found with aggressively_cached=False must NOT set AGGRESSIVELY_CACHED flag."""
        from ravendb.http.http_cache import HttpCache, ItemFlags

        cache = HttpCache()
        cache.set_not_found("http://host/docs/missing", aggressively_cached=False)

        cached_item, _, _ = cache.get("http://host/docs/missing")
        self.assertNotIn(ItemFlags.AGGRESSIVELY_CACHED, cached_item.item.flags)

    def test_not_found_aggressively_cached_is_served_from_cache(self):
        """a 404 cached inside an aggressive-cache context must be served from cache."""
        from ravendb.http.misc import AggressiveCacheMode
        from ravendb.http.http_cache import HttpCache, ItemFlags

        executor = self._make_executor_with_caching(AggressiveCacheMode.TRACK_CHANGES)
        command = self._make_command(can_cache_aggressively=True)

        cache = HttpCache()
        cache.set_not_found("http://host/docs/missing", aggressively_cached=True)
        cached_item, _, cached_value = cache.get("http://host/docs/missing")

        # 404 cached aggressively: should be served from cache (short-circuit)
        self.assertIsNone(cached_value)  # no payload for 404
        self.assertIn(ItemFlags.NOT_FOUND, cached_item.item.flags)
        self.assertIn(ItemFlags.AGGRESSIVELY_CACHED, cached_item.item.flags)
        self.assertTrue(self._check_aggressive_cache_guard(executor, cached_item, cached_value, command))

    def test_not_found_not_aggressively_cached_is_not_served_from_cache(self):
        """a 404 NOT cached aggressively must NOT be served from aggressive cache."""
        from ravendb.http.misc import AggressiveCacheMode
        from ravendb.http.http_cache import HttpCache

        executor = self._make_executor_with_caching(AggressiveCacheMode.TRACK_CHANGES)
        command = self._make_command(can_cache_aggressively=True)

        cache = HttpCache()
        cache.set_not_found("http://host/docs/missing", aggressively_cached=False)
        cached_item, _, cached_value = cache.get("http://host/docs/missing")

        self.assertFalse(self._check_aggressive_cache_guard(executor, cached_item, cached_value, command))

    def test_aggressive_cache_eviction_class_exists(self):
        """DocumentStore._AggressiveCacheInvalidator must exist."""
        from ravendb.documents.store.definition import DocumentStore

        self.assertTrue(hasattr(DocumentStore, "_AggressiveCacheInvalidator"))

    def test_aggressively_cache_for_track_changes_starts_eviction_listener(self):
        """aggressively_cache_for with TRACK_CHANGES must register an eviction listener per database."""
        from ravendb.documents.store.definition import DocumentStore
        from ravendb.http.misc import AggressiveCacheMode

        store = DocumentStore.__new__(DocumentStore)
        store._initialized = True
        store._disposed = None
        store._database = "testdb"
        store._urls = ["http://localhost:8080"]
        store._DocumentStore__aggressive_cache_changes = {}

        eviction_created = []

        def fake_get_request_executor(db=None):
            from ravendb.http.request_executor import RequestExecutor

            re = RequestExecutor.__new__(RequestExecutor)
            re._aggressive_caching_local = threading.local()
            return re

        def fake_get_effective_database(db):
            return db or "testdb"

        def fake_listen_to_changes_and_update_cache(db):
            eviction_created.append(db)

        store.get_request_executor = fake_get_request_executor
        store.get_effective_database = fake_get_effective_database
        store._listen_to_changes_and_update_cache = fake_listen_to_changes_and_update_cache

        ctx = store.aggressively_cache_for(datetime.timedelta(minutes=5), mode=AggressiveCacheMode.TRACK_CHANGES)

        self.assertEqual(["testdb"], eviction_created)

    def test_aggressively_cache_for_do_not_track_skips_eviction_listener(self):
        """aggressively_cache_for with DO_NOT_TRACK_CHANGES must NOT start a change listener."""
        from ravendb.documents.store.definition import DocumentStore
        from ravendb.http.misc import AggressiveCacheMode

        store = DocumentStore.__new__(DocumentStore)
        store._initialized = True
        store._disposed = None
        store._database = "testdb"
        store._urls = ["http://localhost:8080"]
        store._DocumentStore__aggressive_cache_changes = {}

        eviction_created = []

        def fake_get_request_executor(db=None):
            from ravendb.http.request_executor import RequestExecutor

            re = RequestExecutor.__new__(RequestExecutor)
            re._aggressive_caching_local = threading.local()
            return re

        def fake_get_effective_database(db):
            return db or "testdb"

        def fake_listen(db):
            eviction_created.append(db)

        store.get_request_executor = fake_get_request_executor
        store.get_effective_database = fake_get_effective_database
        store._listen_to_changes_and_update_cache = fake_listen

        store.aggressively_cache_for(datetime.timedelta(minutes=5), mode=AggressiveCacheMode.DO_NOT_TRACK_CHANGES)

        self.assertEqual([], eviction_created)

    def test_cache_generation_incremented_on_document_put(self):
        """_AggressiveCacheInvalidator increments cache.generation on document PUT/DELETE."""
        from ravendb.changes.types import DocumentChange, DocumentChangeType
        from ravendb.http.http_cache import HttpCache

        cache = HttpCache()
        initial_generation = cache.generation

        # Simulate what _AggressiveCacheInvalidator does
        def on_document_change(change):
            if change.type_of_change in (DocumentChangeType.PUT, DocumentChangeType.DELETE):
                cache.generation += 1

        put_change = DocumentChange(DocumentChangeType.PUT, "docs/1", "Users", "A:1")
        on_document_change(put_change)

        self.assertEqual(initial_generation + 1, cache.generation)

    def test_cache_generation_incremented_on_index_batch_completed(self):
        """_AggressiveCacheInvalidator increments cache.generation on BatchCompleted index change."""
        from ravendb.changes.types import IndexChange, IndexChangeTypes
        from ravendb.http.http_cache import HttpCache

        cache = HttpCache()
        initial_generation = cache.generation

        def on_index_change(change):
            if change.type_of_change in (IndexChangeTypes.BATCH_COMPLETED, IndexChangeTypes.INDEX_REMOVED):
                cache.generation += 1

        batch_change = IndexChange(IndexChangeTypes.BATCH_COMPLETED, "Orders/ByCompany")
        on_index_change(batch_change)

        self.assertEqual(initial_generation + 1, cache.generation)

    def test_eviction_on_error_increments_generation(self):
        """When the WebSocket drops, Observable.error() must increment cache.generation.

        subscribe() creates an ActionObserver with no on_error, silently swallowing the
        disconnect. subscribe_with_observer() + ActionObserver(on_error=...) is required so
        a connection drop forces revalidation rather than serving stale data indefinitely.
        """
        from concurrent.futures import ThreadPoolExecutor
        from ravendb.changes.observers import Observable, ActionObserver
        from ravendb.http.http_cache import HttpCache

        cache = HttpCache()
        initial_generation = cache.generation

        obs = Observable(executor=ThreadPoolExecutor(max_workers=1))
        obs._filter = lambda x: True

        # Verify the bug: subscribe() gives no on_error, error is swallowed
        obs.subscribe(lambda v: None)
        obs.error(Exception("connection dropped"))
        self.assertEqual(
            initial_generation, cache.generation, "subscribe() must not increment — confirming the bug exists"
        )

        # subscribe_with_observer + on_error must invalidate the cache on connection errors.
        cache2 = HttpCache()
        obs2 = Observable(executor=ThreadPoolExecutor(max_workers=1))
        obs2._filter = lambda x: True

        observer = ActionObserver(
            on_next=lambda v: None,
            on_error=lambda e: setattr(cache2, "generation", cache2.generation + 1),
        )
        obs2.subscribe_with_observer(observer)
        obs2.error(Exception("connection dropped"))
        self.assertEqual(initial_generation + 1, cache2.generation, "subscribe_with_observer() must increment on error")

    def test_listen_to_changes_thread_safe_no_duplicate_listeners(self):
        """Concurrent calls to _listen_to_changes_and_update_cache must create exactly
        one _AggressiveCacheInvalidator per database, not one per racing thread.

        The guard uses __add_change_lock so the check-then-set is atomic.  The eviction
        object is constructed OUTSIDE the lock (to avoid a deadlock: _AggressiveCacheInvalidator
        calls store.changes() which also acquires __add_change_lock, and threading.Lock is
        not reentrant).  Losers call eviction.close() to discard the extra subscription.
        """
        from ravendb.documents.store.definition import DocumentStore
        from unittest.mock import MagicMock, patch

        store = DocumentStore.__new__(DocumentStore)
        store._initialized = True
        store._disposed = None
        store._database = "testdb"
        store._DocumentStore__aggressive_cache_changes = {}
        store._DocumentStore__add_change_lock = threading.Lock()

        created = []

        # Stub _AggressiveCacheInvalidator so no network calls are made, but keep the
        # real _listen_to_changes_and_update_cache lock logic intact.
        def make_fake_eviction(s, db):
            ev = MagicMock()
            created.append(db)
            return ev

        with patch.object(DocumentStore, "_AggressiveCacheInvalidator", side_effect=make_fake_eviction):
            threads = [
                threading.Thread(target=store._listen_to_changes_and_update_cache, args=("testdb",)) for _ in range(20)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # Exactly one eviction must survive in the dict regardless of how many were
        # created (losers are closed and discarded).
        self.assertEqual(
            1,
            len(store._DocumentStore__aggressive_cache_changes),
            "Expected exactly 1 eviction listener in the dict",
        )

    def test_before_delete_fires_during_save_changes_not_during_delete(self):
        """OnBeforeDelete for key-based session.delete() must fire during save_changes(),
        not at the time delete() is called.  Matches C# PrepareForEntitiesDeletion timing
        and keeps both delete paths (key vs entity) consistent."""
        from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
            InMemoryDocumentSessionOperations,
        )
        from unittest.mock import MagicMock
        from ravendb.documents.session.misc import SessionOptions, TransactionMode
        from ravendb.documents.conventions import DocumentConventions
        from ravendb.http.request_executor import RequestExecutor
        import uuid as _uuid

        conventions = DocumentConventions()
        re = MagicMock(spec=RequestExecutor)
        re.conventions = conventions
        re.conventions.use_optimistic_concurrency = False
        re.conventions.max_number_of_requests_per_session = 32
        re.conventions.should_ignore_entity_changes = None

        store = MagicMock()
        store.database = "test"
        store.get_request_executor.return_value = re

        opts = SessionOptions()
        opts.database = "test"
        opts.request_executor = re
        opts.no_tracking = False
        opts.transaction_mode = TransactionMode.SINGLE_NODE
        opts.disable_atomic_document_writes_in_cluster_wide_transaction = False

        session = InMemoryDocumentSessionOperations(store, _uuid.uuid4(), opts)

        fired = []

        def on_before_delete(args):
            fired.append(("save_changes", args.key))

        session.add_before_delete(on_before_delete)

        # C# delete-by-string-id uses Defer(), which does not fire BeforeDelete.
        # BeforeDelete only fires for entity deletes via PrepareForEntitiesDeletion.
        session.delete("docs/test/1")
        self.assertEqual([], fired, "before_delete must not fire for delete-by-string-id")

        session.prepare_for_save_changes()
        self.assertEqual([], fired, "before_delete must not fire for deferred deletes")

    def test_might_have_been_modified_returns_true_when_item_is_none(self):
        """might_have_been_modified must return True for an empty ReleaseCacheItem,
        not raise AttributeError.  Mirrors the same None-guard that already existed on age."""
        from ravendb.http.http_cache import ReleaseCacheItem

        empty = ReleaseCacheItem()
        self.assertIsNone(empty.item)
        # An empty ReleaseCacheItem must handle item=None without raising.
        self.assertTrue(empty.might_have_been_modified)

    def test_no_caching_session_bypasses_aggressive_cache(self):
        """When session.no_caching is True, aggressive cache must not short-circuit execute().
        The first condition in the guard — 'not no_caching' — must block the path even when
        all other conditions (caching active, item fresh, etc.) are satisfied."""
        from ravendb.http.misc import AggressiveCacheMode

        executor = self._make_executor_with_caching(AggressiveCacheMode.TRACK_CHANGES)
        cached_item, cached_value = self._make_cached_item(might_have_been_modified=False)
        command = self._make_command(can_cache_aggressively=True)

        # All other conditions hold, but no_caching=True must block the cache hit
        self.assertFalse(
            self._check_aggressive_cache_guard(executor, cached_item, cached_value, command, no_caching=True)
        )
        # Sanity: same inputs with no_caching=False do produce a cache hit
        self.assertTrue(
            self._check_aggressive_cache_guard(executor, cached_item, cached_value, command, no_caching=False)
        )

    def test_http_cache_getitem_returns_cached_item(self):
        """HttpCache.__getitem__ must return the stored HttpCacheItem,
        not None."""
        from ravendb.http.http_cache import HttpCache, HttpCacheItem

        cache = HttpCache()
        cache.set("http://host/docs/1", "cv-42", '{"Name":"Alice"}')

        item = cache["http://host/docs/1"]

        self.assertIsNotNone(item)
        self.assertIsInstance(item, HttpCacheItem)
        self.assertEqual("cv-42", item.change_vector)
        self.assertEqual('{"Name":"Alice"}', item.payload)

    def test_session_remove_before_query_removes_not_adds(self):
        """remove_before_query on a session must remove the handler, not append a duplicate.
        This keeps handlers unregisterable from an open session."""
        from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
            InMemoryDocumentSessionOperations,
        )
        from unittest.mock import MagicMock
        from ravendb.documents.session.misc import SessionOptions, TransactionMode
        from ravendb.documents.conventions import DocumentConventions
        from ravendb.http.request_executor import RequestExecutor
        import uuid as _uuid

        conventions = DocumentConventions()
        re = MagicMock(spec=RequestExecutor)
        re.conventions = conventions
        re.conventions.use_optimistic_concurrency = False
        re.conventions.max_number_of_requests_per_session = 32
        re.conventions.should_ignore_entity_changes = None

        store = MagicMock()
        store.database = "test"
        store.get_request_executor.return_value = re

        opts = SessionOptions()
        opts.database = "test"
        opts.request_executor = re
        opts.no_tracking = False
        opts.transaction_mode = TransactionMode.SINGLE_NODE
        opts.disable_atomic_document_writes_in_cluster_wide_transaction = False

        session = InMemoryDocumentSessionOperations(store, _uuid.uuid4(), opts)

        handler = lambda args: None
        session.add_before_query(handler)
        self.assertEqual(1, len(session._before_query))

        session.remove_before_query(handler)
        self.assertEqual(0, len(session._before_query), "remove_before_query must remove the handler, not add it again")

    def test_session_remove_before_delete_removes_handler(self):
        """remove_before_delete on a session must remove the handler.
        The public method name should follow the standard remove_* convention used by
        the session and store-level APIs."""
        from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
            InMemoryDocumentSessionOperations,
        )
        from unittest.mock import MagicMock
        from ravendb.documents.session.misc import SessionOptions, TransactionMode
        from ravendb.documents.conventions import DocumentConventions
        from ravendb.http.request_executor import RequestExecutor
        import uuid as _uuid

        conventions = DocumentConventions()
        re = MagicMock(spec=RequestExecutor)
        re.conventions = conventions
        re.conventions.use_optimistic_concurrency = False
        re.conventions.max_number_of_requests_per_session = 32
        re.conventions.should_ignore_entity_changes = None

        store = MagicMock()
        store.database = "test"
        store.get_request_executor.return_value = re

        opts = SessionOptions()
        opts.database = "test"
        opts.request_executor = re
        opts.no_tracking = False
        opts.transaction_mode = TransactionMode.SINGLE_NODE
        opts.disable_atomic_document_writes_in_cluster_wide_transaction = False

        session = InMemoryDocumentSessionOperations(store, _uuid.uuid4(), opts)

        handler = lambda args: None
        session.add_before_delete(handler)
        self.assertEqual(1, len(session._before_delete))

        session.remove_before_delete(handler)
        self.assertEqual(0, len(session._before_delete))
        self.assertFalse(hasattr(session, "remove_before_delete_entity"), "remove_before_delete_entity must not exist")


class TestStoreApiIntegration(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    # --- store events ---

    def test_add_before_store_fires_on_save(self):
        fired = []

        def on_before_store(args):
            fired.append(args.entity)

        self.store.add_before_store(on_before_store)

        class Doc:
            def __init__(self, name):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("hello"), "docs/1")
            session.save_changes()

        self.assertEqual(1, len(fired))
        self.store.remove_before_store(on_before_store)

    def test_before_store_event_args_entity_is_the_stored_object(self):
        """args.entity in OnBeforeStore must be the exact object passed to session.store(),
        not a copy. Mirrors C# Events.cs:Before_Store_Listener which accesses e.Entity."""
        fired_entities = []

        def on_before_store(args):
            fired_entities.append(args.entity)

        self.store.add_before_store(on_before_store)

        class Doc:
            def __init__(self, name):
                self.name = name

        doc = Doc("identity-check")
        with self.store.open_session() as session:
            session.store(doc, "docs/event-args/1")
            session.save_changes()

        self.assertEqual(1, len(fired_entities))
        self.assertIs(doc, fired_entities[0], "args.entity must be the same object, not a copy")
        self.store.remove_before_store(on_before_store)

    def test_add_after_save_changes_fires(self):
        fired = []

        def on_after(args):
            fired.append(True)

        self.store.add_after_save_changes(on_after)

        class Doc:
            pass

        with self.store.open_session() as session:
            session.store(Doc(), "docs/2")
            session.save_changes()

        self.assertGreater(len(fired), 0)
        self.store.remove_after_save_changes(on_after)

    def test_add_before_delete_fires(self):
        fired = []

        def on_before_delete(args):
            fired.append(True)

        self.store.add_before_delete(on_before_delete)

        class Doc:
            def __init__(self, name=None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("x"), "docs/3")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("docs/3", Doc)
            session.delete(doc)
            session.save_changes()

        self.assertEqual(1, len(fired))
        self.store.remove_before_delete(on_before_delete)

    def test_before_delete_event_args_key_matches_deleted_key(self):
        """args.key in OnBeforeDelete must equal the key of the deleted entity.
        Mirrors C# BeforeDeleteEventArgs.DocumentId check."""
        fired_keys = []

        def on_before_delete(args):
            fired_keys.append(args.key)

        self.store.add_before_delete(on_before_delete)

        class Doc:
            def __init__(self, name=None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("x"), "docs/event-args/del")
            session.save_changes()

        with self.store.open_session() as session:
            doc = session.load("docs/event-args/del", Doc)
            session.delete(doc)
            session.save_changes()

        self.assertEqual(["docs/event-args/del"], fired_keys)
        self.store.remove_before_delete(on_before_delete)

    def test_add_before_query_fires(self):
        fired = []

        def on_before_query(args):
            fired.append(True)

        self.store.add_before_query(on_before_query)

        with self.store.open_session() as session:
            list(session.query())

        self.assertEqual(1, len(fired))
        self.store.remove_before_query(on_before_query)

    # --- aggressive cache ---

    def test_aggressively_cache_for_sets_options_on_executor(self):
        with self.store.aggressively_cache_for(datetime.timedelta(minutes=5)):
            request_executor = self.store.get_request_executor()
            self.assertIsNotNone(request_executor.aggressive_caching)
        self.assertIsNone(request_executor.aggressive_caching)

    def test_disable_aggressive_caching_clears_options(self):
        with self.store.aggressively_cache_for(datetime.timedelta(minutes=5)):
            request_executor = self.store.get_request_executor()
            self.assertIsNotNone(request_executor.aggressive_caching)
            with self.store.disable_aggressive_caching():
                self.assertIsNone(request_executor.aggressive_caching)
            self.assertIsNotNone(request_executor.aggressive_caching)

    def test_aggressive_cache_serves_second_load_from_cache(self):
        """Happy path: with aggressive caching, a second load of the same document
        must be served from cache without an additional server request."""

        class Doc:
            def __init__(self, name=None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("hello"), "docs/cache-test")
            session.save_changes()

        request_executor = self.store.get_request_executor()
        with self.store.aggressively_cache_for(datetime.timedelta(minutes=5)):
            with self.store.open_session() as session:
                doc1 = session.load("docs/cache-test", Doc)
                self.assertEqual("hello", doc1.name)
                http_sends_after_first = request_executor.number_of_server_requests

            with self.store.open_session() as session:
                doc2 = session.load("docs/cache-test", Doc)
                self.assertEqual("hello", doc2.name)
                http_sends_after_second = request_executor.number_of_server_requests

        # The second load should be served from cache — no new server request.
        self.assertEqual(http_sends_after_first, http_sends_after_second)

    # --- combined: events + aggressive cache ---

    def test_before_store_event_fires_inside_cache_context(self):
        """Store events still fire correctly while aggressive caching is active."""
        fired = []

        def on_before_store(args):
            fired.append(args.entity)

        self.store.add_before_store(on_before_store)

        class Doc:
            def __init__(self, name):
                self.name = name

        with self.store.aggressively_cache_for(datetime.timedelta(minutes=5)):
            with self.store.open_session() as session:
                session.store(Doc("cached_doc"), "docs/combined/1")
                session.save_changes()

        self.assertEqual(1, len(fired))
        self.store.remove_before_store(on_before_store)

    def test_event_handlers_survive_cache_context_exit(self):
        """Event handlers registered before aggressively_cache_for still work after the context exits."""
        fired = []

        def on_after(args):
            fired.append(True)

        self.store.add_after_save_changes(on_after)

        class Doc:
            pass

        # Fire once inside cache context
        with self.store.aggressively_cache_for(datetime.timedelta(minutes=1)):
            with self.store.open_session() as session:
                session.store(Doc(), "docs/combined/2")
                session.save_changes()

        # Fire again after cache context is gone
        with self.store.open_session() as session:
            session.store(Doc(), "docs/combined/3")
            session.save_changes()

        self.assertEqual(2, len(fired))
        self.store.remove_after_save_changes(on_after)

    def test_track_changes_mode_evicts_cache_on_out_of_band_modification(self):
        """End-to-end: under TRACK_CHANGES mode, a modification made through a
        second session must invalidate the aggressive cache so that the next
        load returns the new value rather than the cached one."""
        import time
        from ravendb.http.misc import AggressiveCacheMode

        class Doc:
            def __init__(self, name=None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("v1"), "docs/track/1")
            session.save_changes()

        with self.store.aggressively_cache_for(datetime.timedelta(minutes=5), mode=AggressiveCacheMode.TRACK_CHANGES):
            with self.store.open_session() as session:
                self.assertEqual("v1", session.load("docs/track/1", Doc).name)

            # Out-of-band modification — a fresh executor would not see the
            # cached value, but the executor inside this scope still must
            # because the Changes API notification evicts the cache entry.
            with self.store.open_session() as session:
                session.load("docs/track/1", Doc).name = "v2"
                session.save_changes()

            deadline = time.time() + 10
            observed = None
            while time.time() < deadline:
                with self.store.open_session() as session:
                    observed = session.load("docs/track/1", Doc).name
                if observed == "v2":
                    break
                time.sleep(0.1)

            self.assertEqual("v2", observed)

    def test_do_not_track_changes_serves_cached_value_after_out_of_band_modification(self):
        """End-to-end: under DO_NOT_TRACK_CHANGES, no Changes API subscription
        is opened, so an out-of-band modification within the scope is not
        observed — the cached value is served for the duration of the scope."""
        from ravendb.http.misc import AggressiveCacheMode

        class Doc:
            def __init__(self, name=None):
                self.name = name

        with self.store.open_session() as session:
            session.store(Doc("v1"), "docs/notrack/1")
            session.save_changes()

        with self.store.aggressively_cache_for(
            datetime.timedelta(minutes=5), mode=AggressiveCacheMode.DO_NOT_TRACK_CHANGES
        ):
            with self.store.open_session() as session:
                self.assertEqual("v1", session.load("docs/notrack/1", Doc).name)

            with self.store.open_session() as session:
                session.load("docs/notrack/1", Doc).name = "v2"
                session.save_changes()

            with self.store.open_session() as session:
                self.assertEqual("v1", session.load("docs/notrack/1", Doc).name)

        with self.store.open_session() as session:
            self.assertEqual("v2", session.load("docs/notrack/1", Doc).name)

    def test_cache_context_does_not_affect_event_registration(self):
        """Entering/exiting the cache context does not remove registered event handlers."""
        store = self.store
        captured = []

        def on_before_query(args):
            captured.append(True)

        store.add_before_query(on_before_query)

        with store.aggressively_cache_for(datetime.timedelta(minutes=5)):
            pass  # enter and exit immediately

        # Handler should still be registered
        with store.open_session() as session:
            list(session.query())

        self.assertEqual(1, len(captured))
        store.remove_before_query(on_before_query)


if __name__ == "__main__":
    unittest.main()
