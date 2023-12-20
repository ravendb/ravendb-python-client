from ravendb import IndexSourceType
from ravendb.documents.indexes.definitions import IndexDefinitionHelper
from ravendb.tests.test_base import TestBase


class TestRavenDB13100(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_detect_documents_index_source_method_syntax(self):
        map_ = "docs.Users.OrderBy(user => user.Id).Select(user => new { user.Name })"
        self.assertEqual(IndexSourceType.DOCUMENTS, IndexDefinitionHelper.detect_static_index_source_type(map_))

    def test_can_detect_time_series_index_source_method_syntax(self):
        map_ = (
            "timeSeries.Companies.SelectMany(ts => ts.Entries, (ts, entry) => new {"
            "   HeartBeat = entry.Values[0], "
            "   Date = entry.Timestamp.Date, "
            "   User = ts.DocumentId "
            "});"
        )
        self.assertEqual(IndexSourceType.TIME_SERIES, IndexDefinitionHelper.detect_static_index_source_type(map_))

    def test_can_detect_time_series_index_source_linq_syntax_single_ts(self):
        map_ = "from ts in timeSeries.Users"
        self.assertEqual(IndexSourceType.TIME_SERIES, IndexDefinitionHelper.detect_static_index_source_type(map_))

    def test_can_detect_time_series_index_source_linq_syntax_can_strip_white_space(self):
        map_ = "\t\t  \t from    ts  \t \t in  \t \t timeSeries.Users"
        self.assertEqual(IndexSourceType.TIME_SERIES, IndexDefinitionHelper.detect_static_index_source_type(map_))

    def test_can_detect_time_series_index_source_linq_syntax_all_ts(self):
        map_ = "from ts in timeSeries"
        self.assertEqual(IndexSourceType.TIME_SERIES, IndexDefinitionHelper.detect_static_index_source_type(map_))
