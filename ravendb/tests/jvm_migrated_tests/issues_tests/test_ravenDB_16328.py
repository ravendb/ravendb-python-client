import unittest
from datetime import timedelta
from typing import Type

from ravendb import SorterDefinition, AbstractIndexCreationTask, AnalyzerDefinition, DocumentStore
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb.documents.operations.indexes import ResetIndexOperation
from ravendb.infrastructure.orders import Company
from ravendb.serverwide.operations.analyzers import PutServerWideAnalyzersOperation, DeleteServerWideAnalyzerOperation
from ravendb.serverwide.operations.sorters import PutServerWideSortersOperation, DeleteServerWideSorterOperation
from ravendb.tests.test_base import TestBase


sorter_code = (
    "using System;\n"
    "using System.Collections.Generic;\n"
    "using Lucene.Net.Index;\n"
    "using Lucene.Net.Search;\n"
    "using Lucene.Net.Store;\n"
    "\n"
    "namespace SlowTests.Data.RavenDB_8355\n"
    "{\n"
    "    public class MySorter : FieldComparator\n"
    "    {\n"
    "        private readonly string _args;\n"
    "\n"
    "        public MySorter(string fieldName, int numHits, int sortPos, bool reversed, List<string> diagnostics)\n"
    "        {\n"
    '            _args = $"{fieldName}:{numHits}:{sortPos}:{reversed}";\n'
    "        }\n"
    "\n"
    "        public override int Compare(int slot1, int slot2)\n"
    "        {\n"
    '            throw new InvalidOperationException($"Catch me: {_args}");\n'
    "        }\n"
    "\n"
    "        public override void SetBottom(int slot)\n"
    "        {\n"
    '            throw new InvalidOperationException($"Catch me: {_args}");\n'
    "        }\n"
    "\n"
    "        public override int CompareBottom(int doc, IState state)\n"
    "        {\n"
    '            throw new InvalidOperationException($"Catch me: {_args}");\n'
    "        }\n"
    "\n"
    "        public override void Copy(int slot, int doc, IState state)\n"
    "        {\n"
    '            throw new InvalidOperationException($"Catch me: {_args}");\n'
    "        }\n"
    "\n"
    "        public override void SetNextReader(IndexReader reader, int docBase, IState state)\n"
    "        {\n"
    '            throw new InvalidOperationException($"Catch me: {_args}");\n'
    "        }\n"
    "\n"
    '        public override IComparable this[int slot] => throw new InvalidOperationException($"Catch me: {_args}");\n'
    "    }\n"
    "}\n"
)


class TestRavenDB16328(TestBase):
    def setUp(self):
        super().setUp()

    @unittest.skip("Skipping due to license on CI/CD")
    def test_can_use_custom_sorter(self):
        with self.store.open_session() as session:
            c1 = Company(name="C1")
            c2 = Company(name="C2")
            session.store(c1)
            session.store(c2)
            session.save_changes()

        sorter_name = self.store.database

        sorter_code = self.get_sorter(sorter_name)

        sorter_definition = SorterDefinition(sorter_name, sorter_code)
        self.store.maintenance.server.send(PutServerWideSortersOperation(sorter_definition))

        # checking if we can send again same sorter
        self.store.maintenance.server.send(PutServerWideSortersOperation(sorter_definition))

        sorter_code = sorter_code.replace("Catch me", "Catch me 2")

        # checking if we can update sorter
        updated_sorter = SorterDefinition(sorter_name, sorter_code)
        self.store.maintenance.server.send(PutServerWideSortersOperation(updated_sorter))

        # we should not able to add sorter with non-matching name
        final_sorter_code = sorter_code
        with self.assertRaises(RuntimeError):
            invalid_sorter = SorterDefinition()
            invalid_sorter.name = f"{sorter_name}_OtherName"
            invalid_sorter.code = final_sorter_code
            self.store.maintenance.server.send(PutServerWideSortersOperation(invalid_sorter))

        self.store.maintenance.server.send(DeleteServerWideSorterOperation(sorter_name))

    @staticmethod
    def get_sorter(name) -> str:
        return sorter_code.replace("MySorter", name)

    analyzer = (
        "using System.IO;\n"
        "using Lucene.Net.Analysis;\n"
        "using Lucene.Net.Analysis.Standard;\n"
        "\n"
        "namespace SlowTests.Data.RavenDB_14939\n"
        "{\n"
        "    public class MyAnalyzer : StandardAnalyzer\n"
        "    {\n"
        "        public MyAnalyzer()\n"
        "            : base(Lucene.Net.Util.Version.LUCENE_30)\n"
        "        {\n"
        "        }\n"
        "\n"
        "        public override TokenStream TokenStream(string fieldName, TextReader reader)\n"
        "        {\n"
        "            return new ASCIIFoldingFilter(base.TokenStream(fieldName, reader));\n"
        "        }\n"
        "    }\n"
        "}\n"
    )

    @staticmethod
    def _fill(store: DocumentStore) -> None:
        with store.open_session() as session:
            session.store(Customer(name="Rogério"))
            session.store(Customer(name="Rogerio"))
            session.store(Customer(name="Paulo Rogerio"))
            session.store(Customer(name="Paulo Rogério"))
            session.save_changes()

    def _assert_count(
        self, store: DocumentStore, index: Type[AbstractIndexCreationTask], expected_count: int = 4
    ) -> None:
        self.wait_for_indexing(store)

        with store.open_session() as session:
            results = list(session.query_index_type(index, Customer).no_caching().search("name", "Rogério*"))
            self.assertEqual(expected_count, len(results))

    def test_can_use_custom_analyzer(self):
        analyzer_name = "MyAnalyzer"

        self.assertRaisesWithMessageContaining(
            self.store.execute_index,
            Exception,
            "Cannot find analyzer type '" + analyzer_name + "' for field: name",
            self.MyIndex(analyzer_name),
        )

        try:
            analyzer_definition = AnalyzerDefinition()
            analyzer_definition.name = analyzer_name
            analyzer_definition.code = self.analyzer

            self.store.maintenance.server.send(PutServerWideAnalyzersOperation(analyzer_definition))

            self.store.execute_index(self.MyIndex(analyzer_name))

            self._fill(self.store)

            self.wait_for_indexing(self.store)

            self._assert_count(self.store, self.MyIndex)

            self.store.maintenance.server.send(DeleteServerWideAnalyzerOperation(analyzer_name))

            self.store.maintenance.send(ResetIndexOperation(self.MyIndex().index_name))

            errors = self.wait_for_indexing_errors(self.store, timedelta(seconds=10))
            self.assertEqual(1, len(errors))
            self.assertEqual(1, len(errors[0].errors))
            self.assertIn(
                "Cannot find analyzer type '" + analyzer_name + "' for field: name", errors[0].errors[0].error
            )
        finally:
            self.store.maintenance.server.send(DeleteServerWideAnalyzerOperation(analyzer_name))

    class MyIndex(AbstractIndexCreationTask):
        def __init__(self, analyzer_name: str = "MyAnalyzer"):
            super().__init__()
            self.map = "from customer in docs.Customers select new { customer.name }"

            self._index("name", FieldIndexing.SEARCH)
            self._analyze("name", analyzer_name)


class Customer:
    def __init__(self, Id: str = None, name: str = None):
        self.Id = Id
        self.name = name
