from ravendb import SorterDefinition
from ravendb.infrastructure.orders import Company
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
