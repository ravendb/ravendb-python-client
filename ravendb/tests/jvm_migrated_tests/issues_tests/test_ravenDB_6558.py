from typing import List, Optional

from ravendb import HighlightingOptions
from ravendb.documents.indexes.definitions import FieldIndexing, FieldStorage, FieldTermVector
from ravendb.documents.indexes.index_creation import AbstractMultiMapIndexCreationTask
from ravendb.documents.queries.highlighting import Highlightings
from ravendb.tests.test_base import TestBase


class EventItem:
    def __init__(self, Id: str = None, title: str = None, slug: str = None, content: str = None):
        self.Id = Id
        self.title = title
        self.slug = slug
        self.content = content


class SearchResults:
    def __init__(self, result: EventItem, highlights: List[str] = None, title: str = None):
        self.result = result
        self.highlights = highlights
        self.title = title


class ContentSearchIndex(AbstractMultiMapIndexCreationTask):
    def __init__(self):
        super(ContentSearchIndex, self).__init__()

        self._add_map(
            "docs.EventItems.Select(doc => new {\n"
            "    doc = doc,\n"
            "    slug = Id(doc).ToString().Substring(Id(doc).ToString().IndexOf('/') + 1)\n"
            "}).Select(this0 => new {\n"
            "    slug = this0.slug,\n"
            "    title = this0.doc.title,\n"
            "    content = this0.doc.content\n"
            "})"
        )

        self._index("slug", FieldIndexing.SEARCH)
        self._store("slug", FieldStorage.YES)
        self._term_vector("slug", FieldTermVector.WITH_POSITIONS_AND_OFFSETS)

        self._index("title", FieldIndexing.SEARCH)
        self._store("title", FieldStorage.YES)
        self._term_vector("title", FieldTermVector.WITH_POSITIONS_AND_OFFSETS)

        self._index("content", FieldIndexing.SEARCH)
        self._store("content", FieldStorage.YES)
        self._term_vector("content", FieldTermVector.WITH_POSITIONS_AND_OFFSETS)


class TestRavenDB6558(TestBase):
    def setUp(self):
        super(TestRavenDB6558, self).setUp()

    def test_can_use_different_pre_and_post_tags_per_field(self):
        with self.store.open_session() as session:
            events_item = EventItem()
            events_item.slug = "ravendb-indexes-explained"
            events_item.title = "RavenDB indexes explained"
            events_item.content = (
                "Itamar Syn-Hershko: Afraid of Map/Reduce? "
                "In this session, core RavenDB developer Itamar Syn-Hershko will walk through "
                "the RavenDB indexing process, grok it and much more."
            )
            session.store(events_item, "items/1")
            session.save_changes()

        ContentSearchIndex().execute(self.store)

        options1 = HighlightingOptions()
        options1.pre_tags = ["***"]
        options1.post_tags = ["***"]

        options2 = HighlightingOptions()
        options2.pre_tags = ["^^^"]
        options2.post_tags = ["^^^"]

        title_highlightings: Optional[Highlightings] = None

        def _title_highlighting_callback(title_highlight: Highlightings):
            nonlocal title_highlightings
            title_highlightings = title_highlight

        content_highlightings: Optional[Highlightings] = None

        def _content_highlighting_callback(content_highlight: Highlightings):
            nonlocal content_highlightings
            content_highlightings = content_highlight

        with self.store.open_session() as session:
            results = list(
                session.query_index("ContentSearchIndex", EventItem)
                .wait_for_non_stale_results()
                .highlight("title", 128, 2, _title_highlighting_callback, options1)
                .highlight("content", 128, 2, _content_highlighting_callback, options2)
                .search("title", "RavenDB")
                .boost(12)
                .search("content", "RavenDB")
            )

            self.assertIn("***", title_highlightings.get_fragments("items/1")[0])
            self.assertIn("^^^", content_highlightings.get_fragments("items/1")[0])
