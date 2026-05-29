"""
Unit tests for RavenDocumentQuery.Now / Today / CmpXchg plus the
WhereToken NOW/TODAY MethodsType output.
"""

import unittest
import warnings

from ravendb.documents.queries.raven_document_query import RavenDocumentQuery
from ravendb.documents.session.misc import CmpXchg, MethodCall
from ravendb.documents.session.tokens.misc import WhereOperator
from ravendb.documents.session.tokens.query_tokens.definitions import WhereToken


class TestRavenDocumentQueryFactories(unittest.TestCase):
    def test_now_no_args(self):
        t = RavenDocumentQuery.now()
        self.assertEqual(WhereToken.MethodsType.NOW, t.method_type)
        self.assertEqual([], t.args)
        self.assertIsInstance(t, MethodCall)

    def test_now_with_offset(self):
        t = RavenDocumentQuery.now("+1d")
        self.assertEqual(WhereToken.MethodsType.NOW, t.method_type)
        self.assertEqual(["+1d"], t.args)

    def test_today(self):
        t = RavenDocumentQuery.today()
        self.assertEqual(WhereToken.MethodsType.TODAY, t.method_type)
        self.assertEqual([], t.args)

    def test_cmp_xchg_does_not_warn(self):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            x = RavenDocumentQuery.cmp_xchg("foo")
            self.assertFalse(any(issubclass(w.category, DeprecationWarning) for w in captured))
        self.assertIsInstance(x, CmpXchg)
        self.assertEqual(["foo"], x.args)

    def test_legacy_cmp_xchg_value_emits_deprecation(self):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            CmpXchg.value("foo")
            self.assertTrue(any(issubclass(w.category, DeprecationWarning) for w in captured))


class TestWhereTokenNowTodayOutput(unittest.TestCase):
    @staticmethod
    def _render(method_type: WhereToken.MethodsType, parameters):
        token = WhereToken.create(
            WhereOperator.GREATER_THAN,
            "CreatedAt",
            None,
            WhereToken.WhereOptions(method_type__parameters__property__exact=(method_type, parameters, None, None)),
        )
        out = []
        token.write_to(out)
        return "".join(out)

    def test_now_no_param(self):
        self.assertEqual("CreatedAt > now()", self._render(WhereToken.MethodsType.NOW, []))

    def test_now_with_param(self):
        self.assertEqual("CreatedAt > now($p0)", self._render(WhereToken.MethodsType.NOW, ["p0"]))

    def test_today(self):
        self.assertEqual("CreatedAt > today()", self._render(WhereToken.MethodsType.TODAY, []))


if __name__ == "__main__":
    unittest.main()
