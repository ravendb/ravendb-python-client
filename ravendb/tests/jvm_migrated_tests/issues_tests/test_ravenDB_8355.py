from typing import Type

from ravendb import SorterDefinition, DocumentStore, RawDocumentQuery, DocumentQuery
from ravendb.documents.operations.sorters import PutSortersOperation
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.infrastructure.orders import Company
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


class TestRavenDB8355(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_use_custom_sorter(self):
        sorter_definition = SorterDefinition("MySorter", sorter_code)
        operation = PutSortersOperation(sorter_definition)

        self.store.maintenance.send(operation)

        with self.store.open_session() as session:
            company1 = Company(name="C1")
            session.store(company1)

            company2 = Company(name="C2")
            session.store(company2)

            session.save_changes()

        self._can_use_sorter_internal(RuntimeError, self.store, "Catch me: name:2:0:False", "Catch me: name:2:0:True")

    def _can_use_sorter_internal(self, expected_class: Type[Exception], store: DocumentStore, asc: str, desc: str) -> None:
        with store.open_session() as session:
            self.assertRaisesWithMessageContaining(
                RawDocumentQuery.__iter__,
                expected_class,
                asc,
                session.advanced.raw_query("from Companies order by custom(name, 'MySorter')"),
            )
            self.assertRaisesWithMessageContaining(
                DocumentQuery.__iter__,
                expected_class,
                asc,
                session.query(object_type=Company).order_by(field="name", sorter_name_or_ordering_type="MySorter"),
            )

            self.assertRaisesWithMessageContaining(
                RawDocumentQuery.__iter__,
                expected_class,
                desc,
                session.advanced.raw_query("from Companies order by custom(name, 'MySorter') desc"),
            )
            self.assertRaisesWithMessageContaining(
                DocumentQuery.__iter__,
                expected_class,
                desc,
                session.query(object_type=Company).order_by_descending(
                    field="name", sorter_name_or_ordering_type="MySorter"
                ),
            )
