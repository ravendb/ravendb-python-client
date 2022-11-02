from typing import Optional

from ravendb import AbstractIndexCreationTask, Explanations, ExplanationOptions
from ravendb.documents.indexes.definitions import FieldStorage
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class Companies_ByName(AbstractIndexCreationTask):
    class Result:
        def __init__(self, Id: str = None, key: str = None, count: int = None):
            self.Id = Id
            self.key = key
            self.count = count

    def __init__(self):
        super(Companies_ByName, self).__init__()
        self.map = "from c in docs.Companies select new { key = c.name, count = 1 }"
        self.reduce = (
            "from result in results "
            "group result by result.key "
            "into g "
            "select new "
            "{ "
            "  key = g.Key, "
            "  count = g.Sum(x => x.count) "
            "}"
        )
        self._store("key", FieldStorage.YES)


class TestRavenDB9745(TestBase):
    def setUp(self):
        super(TestRavenDB9745, self).setUp()

    def test_explain(self):
        Companies_ByName().execute(self.store)

        with self.store.open_session() as session:
            company1 = Company(name="Micro")
            company2 = Company(name="Microsoft")
            company3 = Company(name="Google")

            session.store(company1)
            session.store(company2)
            session.store(company3)

            session.save_changes()

        self.wait_for_indexing(self.store)

        explanations: Optional[Explanations] = None

        def _callback(expl: Explanations):
            nonlocal explanations
            explanations = expl

        with self.store.open_session() as session:
            companies = list(
                session.advanced.document_query(object_type=Company)
                .include_explanations(explanations_callback=_callback)
                .search("name", "Micro*")
            )

            self.assertEqual(2, len(companies))

            exp = explanations.explanations[companies[0].Id]
            self.assertIsNotNone(exp)

            exp = explanations.explanations[companies[1].Id]
            self.assertIsNotNone(exp)

        with self.store.open_session() as session:
            options = ExplanationOptions(group_key="key")
            results = list(
                session.advanced.document_query_from_index_type(
                    Companies_ByName, Companies_ByName.Result
                ).include_explanations(options, _callback)
            )

            self.assertEqual(3, len(results))

            exp = explanations.explanations[results[0].key]
            self.assertIsNotNone(exp)

            exp = explanations.explanations[results[1].key]
            self.assertIsNotNone(exp)

            exp = explanations.explanations[results[2].key]
            self.assertIsNotNone(exp)
