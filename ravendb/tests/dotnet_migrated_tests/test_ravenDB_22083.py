import http
import unittest

from ravendb import AbstractIndexCreationTask, IndexDefinition, PutIndexesOperation
from ravendb.exceptions.compilation import CompilationException
from ravendb.exceptions.documents.compilation import IndexCompilationException
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.tests.test_base import TestBase


class Candle:
    def __init__(self, Id: str = None, volume: float = None, time: int = None):
        self.Id = Id
        self.volume = volume
        self.time = time


class AggregateCandle:
    def __init__(self, reference: str = None, volume: float = None, timeframe: str = None):
        self.reference = reference
        self.volume = volume
        self.timeframe = timeframe


class IndexWithConstantArrayAsInnerSource(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from candle in docs.Candles "
            'from x in new[] { new { Interval = 60000L, Timeframe = "1m" }, '
            'new { Interval = 300000L, Timeframe = "5m" } } '
            'select new { reference = $"{candle.time / x.Interval}", volume = candle.volume, timeframe = x.Timeframe }'
        )
        self.reduce = (
            "from result in results "
            "group result by new { result.timeframe, result.reference } into g "
            "select new { reference = g.Key.reference, volume = g.Sum(x => x.volume), timeframe = g.Key.timeframe }"
        )


class IndexWithConstantArrayAsOuterSource(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            'from x in new[] { new { Interval = 60000L, Timeframe = "1m" }, '
            'new { Interval = 300000L, Timeframe = "5m" } } '
            "from candle in docs.Candles "
            'select new { reference = $"{candle.time / x.Interval}", volume = candle.volume, timeframe = x.Timeframe }'
        )


class IndexWithOrderedReduce(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = (
            "from candle in docs.Candles "
            'select new { reference = $"{candle.time / 60000}", volume = candle.volume, timeframe = "1m" }'
        )
        self.reduce = (
            "results.GroupBy(x => new { x.timeframe, x.reference })"
            ".Select(g => new { reference = g.Key.reference, volume = g.Sum(x => x.volume), timeframe = g.Key.timeframe })"
            ".OrderBy(x => x.reference)"
        )


class TestIndexCompilationExceptionDispatch(unittest.TestCase):
    def _dispatch(self, type_as_string: str, json_body: dict = None) -> RavenException:
        schema = ExceptionDispatcher.ExceptionSchema(
            url="http://localhost:8080",
            object_type=type_as_string,
            message="Failed to compile index 'Index1'",
            error="Failed to compile index 'Index1'",
        )
        return ExceptionDispatcher.get(schema, http.HTTPStatus.INTERNAL_SERVER_ERROR, json_body=json_body)

    def test_index_compilation_exception_is_typed_and_filled(self):
        exception = self._dispatch(
            "Raven.Client.Exceptions.Documents.Compilation.IndexCompilationException",
            {"IndexDefinitionProperty": "Maps", "ProblematicText": "from x in items select x"},
        )

        self.assertIsInstance(exception, IndexCompilationException)
        self.assertIsInstance(exception, CompilationException)
        self.assertEqual("Maps", exception.index_definition_property)
        self.assertEqual("from x in items select x", exception.problematic_text)
        self.assertIn("Failed to compile index 'Index1'", str(exception))

    def test_index_compilation_exception_without_body(self):
        exception = self._dispatch("Raven.Client.Exceptions.Documents.Compilation.IndexCompilationException")

        self.assertIsInstance(exception, IndexCompilationException)
        self.assertIsNone(exception.index_definition_property)
        self.assertIsNone(exception.problematic_text)

    def test_compilation_exception_is_typed(self):
        exception = self._dispatch("Raven.Client.Exceptions.Compilation.CompilationException")

        self.assertIs(CompilationException, type(exception))


class TestRavenDB22083(TestBase):

    def _store_candles(self):
        with self.store.open_session() as session:
            session.store(Candle(volume=1, time=60000))
            session.store(Candle(volume=2, time=60000))
            session.save_changes()

    def test_index_with_constant_array_as_inner_source_deploys_and_indexes(self):
        self.store.execute_index(IndexWithConstantArrayAsInnerSource())
        self._store_candles()
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(
                session.query_index_type(IndexWithConstantArrayAsInnerSource, AggregateCandle).where_equals(
                    "timeframe", "1m"
                )
            )

            self.assertEqual(1, len(results))
            self.assertEqual(3, results[0].volume)

    def test_index_with_ordered_reduce_deploys_and_indexes(self):
        self.store.execute_index(IndexWithOrderedReduce())
        self._store_candles()
        self.wait_for_indexing(self.store)

        with self.store.open_session() as session:
            results = list(session.query_index_type(IndexWithOrderedReduce, AggregateCandle))

            self.assertEqual(1, len(results))
            self.assertEqual(3, results[0].volume)

    def test_index_with_constant_array_as_outer_source_is_rejected_at_compilation(self):
        with self.assertRaises(IndexCompilationException) as context:
            self.store.execute_index(IndexWithConstantArrayAsOuterSource())

        self.assertEqual("Maps", context.exception.index_definition_property)
        self.assertIn("must start its enumeration from 'docs'", str(context.exception))

    def test_index_definition_sent_as_text_with_constant_array_as_outer_source_throws(self):
        index_definition = IndexDefinition()
        index_definition.name = "CandlesByTimeframeFromText"
        index_definition.maps = {
            'from x in new[] { new { Timeframe = "1m", Interval = 60000L }, '
            'new { Timeframe = "5m", Interval = 300000L } } '
            "from candle in docs.Candles "
            'select new { Ref = $"{candle.Time / x.Interval}", Volume = candle.Volume, Timeframe = x.Timeframe }'
        }

        with self.assertRaises(IndexCompilationException) as context:
            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertIn("Failed to compile index 'CandlesByTimeframeFromText'", str(context.exception))
        self.assertIn("must start its enumeration from 'docs'", str(context.exception))
        self.assertEqual("Maps", context.exception.index_definition_property)

    def test_index_definition_sent_as_text_not_rooted_in_documents_source_throws(self):
        index_definition = IndexDefinition()
        index_definition.name = "CandlesFromMethodSyntaxText"
        index_definition.maps = {
            "new[] { 60000L, 300000L }.SelectMany(t => docs.Candles, "
            "(t, candle) => new { Ref = candle.Time / t, Volume = candle.Volume })"
        }

        with self.assertRaises(IndexCompilationException) as context:
            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual("Maps", context.exception.index_definition_property)
        self.assertIn("compiled as JavaScript", str(context.exception))
        self.assertIn("a C# map must start its enumeration from one of these sources", str(context.exception))

    def test_javascript_index_with_invalid_map_throws(self):
        index_definition = IndexDefinition()
        index_definition.name = "BrokenJsMap"
        index_definition.maps = {"map('Candles', function (c) { return { Volume: c.Volume }; )"}

        with self.assertRaises(IndexCompilationException) as context:
            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual("Maps", context.exception.index_definition_property)

    def test_javascript_index_with_invalid_reduce_throws(self):
        index_definition = IndexDefinition()
        index_definition.name = "BrokenJsReduce"
        index_definition.maps = {"map('Candles', function (c) { return { Volume: c.Volume }; })"}
        index_definition.reduce = (
            "groupBy(x => x.Volume).aggregate(g => { return { Volume: g.values.reduce((a, b) => a + b.Volume, 0) }; )"
        )

        with self.assertRaises(IndexCompilationException) as context:
            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertEqual("Reduce", context.exception.index_definition_property)
